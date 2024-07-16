{-# language BangPatterns #-}
{-# language KindSignatures #-}
{-# language TypeOperators #-}
{-# language DataKinds #-}
{-# language DeriveFunctor #-}
{-# language DerivingStrategies #-}
{-# language ScopedTypeVariables #-}
{-# language LambdaCase #-}
{-# language NumericUnderscores #-}
{-# language DuplicateRecordFields #-}
{-# language OverloadedRecordDot #-}

module Channel
  ( M
  , run
  , with
  , withExisting
  , throw
  , throwProtocolException
  , throwErrorCode
  , lift
  , exchangeBytes
  , lookupHostname
  , lookupPort
  , substitute
    -- * Exceptions
  , KafkaException(..)
  , CommunicationException(..)
  , Description(..)
  ) where

import ChannelSig (Resource,SendException,ReceiveException,ConnectException)

import Arithmetic.Types (Fin(Fin),type (:=:))
import Control.Monad (when)
import Control.Monad.Trans.Except (ExceptT(ExceptT),runExceptT,throwE)
import Data.Bytes.Chunks (Chunks)
import Data.Bytes.Types (Bytes(Bytes))
import Data.Int (Int32,Int16)
import Data.Primitive (ByteArray)
import Data.Primitive (SmallArray,PrimArray)
import Data.Text (Text)
import Data.Word (Word16)
import GHC.TypeNats (Nat,type (+))
import Kafka.ApiKey (ApiKey)
import Kafka.ErrorCode (ErrorCode)
import Kafka.Exchange.Types (Broker)
import Kafka.Exchange.Types (ProtocolException,Correlated(Correlated),Broker(Broker))
import Kafka.Parser.Context (Context)

import qualified Data.Primitive.Contiguous as C
import qualified Kafka.Message.Request.V2 as Message.Request
import qualified Data.Primitive as PM
import qualified Data.Primitive.ByteArray.BigEndian as BigEndian
import qualified Kafka.Header.Response.V0 as Header.Response.V0
import qualified Kafka.Header.Response.V1 as Header.Response.V1
import qualified Arithmetic.Nat as Nat
import qualified Data.Bytes.Parser as Parser
import qualified Kafka.Parser.Context as Ctx
import qualified ChannelSig
-- import qualified Error

import qualified Kafka.Exchange.Types as Types

-- | The type @e@ is a way for anything that builds on top of this
-- to introduce additional exceptions.
data KafkaException e
  = Connect
      !Text -- broker hostname
      !Word16 -- broker port
      !ConnectException -- error while connecting (often errno from connect)
  | Communicate !CommunicationException
  | Application !e

instance Show e => Show (KafkaException e) where
  showsPrec d (Communicate e) = showParen (d > 10)         
    (showString "Communicate " . showsPrec 11 e)
  showsPrec d (Application e) = showParen (d > 10)         
    (showString "Application " . showsPrec 11 e)
  showsPrec d (Connect host thePort e) = showParen (d > 10)
    (showString "Connect " . shows host . showChar ' ' . shows thePort . showChar ' ' . ChannelSig.showsPrecConnectException 11 e)

data CommunicationException = CommunicationException
  { apiKey :: !ApiKey
  , hostname :: !Text
    -- ^ Which broker were we communicating with when the failure happened.
  , port :: !Word16
    -- ^ Which port was used to talk with the broker.
  , correlationId :: !Int32
  , description :: !Description
  } deriving (Show)

data Description
  = Send !SendException
  | Receive !ReceiveException
  | Protocol !ProtocolException
  | ErrorCode !Context !ErrorCode

instance Show Description where
  showsPrec d (Protocol e) = showParen (d > 10)         
    (showString "Protocol " . showsPrec 11 e)
  showsPrec d (ErrorCode a b) = showParen (d > 10)         
    (showString "ErrorCode " . showsPrec 11 a . showString " " . showsPrec 11 b)
  showsPrec d (Send e) = showParen (d > 10)
    (showString "Send " . ChannelSig.showsPrecSendException 11 e)
  showsPrec d (Receive e) = showParen (d > 10)
    (showString "Receive " . ChannelSig.showsPrecReceiveException 11 e)


data Result a = Result
  !(SmallArray Env) -- next correlation ids. hostnames and sockets should not change
  !a
  deriving stock (Functor)

data Env = Env
  !Text -- hostname
  !Word16 -- port
  !Resource -- probably a socket
  !Int32 -- correlation id

newtype M e (n :: Nat) a = M
  (    PrimArray Int -- Length m. Indices (>=0, <n) into environment array
    -> SmallArray Env -- Length n
    -> Text -- client id, shared by all sessions
    -> ChannelSig.M (Either (KafkaException e) (Result a))
  )
  deriving stock (Functor)

bindM :: M e n a -> (a -> M e n b) -> M e n b
bindM (M f) g = M $ \ixs envs clientId -> do
  f ixs envs clientId >>= \case
    Left err -> pure (Left err)
    Right (Result envs' a) -> case g a of
      M h -> h ixs envs' clientId

pureM :: a -> M e n a
pureM a = M (\_ envs _ -> pure (Right (Result envs a)))

instance Applicative (M e n) where
  pure = pureM
  f <*> a = f `bindM` \f' -> a `bindM` \a' -> pureM (f' a')

instance Monad (M e n) where
  (>>=) = bindM

-- | Lift a computation using the base monadic type constructor into
-- a computation using the kafka client monadic type constructor.
-- In practice, the effective type signature of this will be:
--
-- > lift :: IO a -> M e n a
lift :: ChannelSig.M a -> M e n a
lift m = M
  (\_ envs _ -> do
    a <- m 
    pure (Right (Result envs a))
  )

-- | Run an action in which an addtional broker is available. If there
-- is already a connection to the broker, this reuses the existing
-- connection rather than establishing an additional connection.
-- Put otherwise, it is possible for logical sessions at indicies
-- @i@ and @j@ (where @i != j@) to share a single TCP session.
--
-- The indices are debruijn style. Consider:
--
-- > with brokerA $ with brokerB $ action
--
-- In @action@, 0 refers to brokerB and 1 refers to brokerA.
with ::
     Broker -- ^ Hostname or IP address of broker
  -> M e (n + 1) a -- ^ Action in which additional broker is available
  -> M e n a
with (Broker host thePort) (M f) = M $ \ixs envs clientId ->
  case C.findIndex (\(Env existingHost existingPort _ _) -> existingHost == host && existingPort == thePort) envs of
    Nothing -> ChannelSig.withConnection host thePort $ \r -> case r of
      Left e -> pure (Left (Connect host thePort e))
      Right resource ->
        let !env = Env host thePort resource 0
            !envs' = C.insertAt envs (C.size envs) env
            !ixs' = C.insertAt ixs 0 (C.size envs)
         in f ixs' envs' clientId
    Just ix ->
      let ixs' = C.insertAt ixs 0 ix
       in f ixs' envs clientId

-- | Variant of 'with' that fails with XYZ if there is not already a connection
-- to the broker in the context. This can be useful for a producer. With a
-- producer, one possible context-management strategy is to begin by
-- connecting to all brokers (on a five-node cluster, then would create
-- a context of type @M e 5@). Then, before each produce request, use
-- @withExisting@ to use the right broker.
withExisting ::
     Broker -- ^ Hostname or IP address of broker
  -> M e (n + 1) a -- ^ Action in which additional broker is available
  -> M e n a
withExisting _ _ = error "withExisting: write this. I don't actually need it yet"

run ::
     Text -- ^ Client id
  -> M e 0 a -- ^ Context with zero connections 
  -> ChannelSig.M (Either (KafkaException e) a)
run clientId (M f) = f mempty mempty clientId >>= \case
  Left e -> pure (Left e)
  Right (Result _ a) -> pure (Right a)

-- | Get the hostname used for the connection.
lookupHostname :: Fin n -> M e n Text
lookupHostname (Fin ix _) = M $ \ixs envs _ -> runExceptT $ do
  let !envIx = PM.indexPrimArray ixs (Nat.demote ix)
  let Env host _ _ _ = PM.indexSmallArray envs envIx
  pure (Result envs host)

-- | Get the hostname used for the connection.
lookupPort :: Fin n -> M e n Word16
lookupPort (Fin ix _) = M $ \ixs envs _ -> runExceptT $ do
  let !envIx = PM.indexPrimArray ixs (Nat.demote ix)
  let Env _ thePort _ _ = PM.indexSmallArray envs envIx
  pure (Result envs thePort)

-- An exchange of raw bytes. No encoding or decoding is performed. We return
-- the correlation ID used in the exchange so that, in the event of a decode
-- failure, the correlation ID is available for inclusion.
exchangeBytes ::
     Fin n
  -> ApiKey
  -> Int16 -- API version
  -> Int16 -- Response header version. Must be 0 or 1.
  -> Chunks -- The inner request, already encoded, without the header
  -> M e n (Correlated Bytes) -- Response bytes (after stripping response header) and the correlation id
exchangeBytes (Fin ix _) !key !version !respHeaderVersion inner = M $ \ixs envs clientId -> runExceptT $ do
  let !envIx = PM.indexPrimArray ixs (Nat.demote ix)
  let Env host thePort resource corrId = PM.indexSmallArray envs envIx
  let !hdr = Message.Request.Header
        { apiKey = key
        , apiVersion = version
        , correlationId = corrId
        , clientId = Just clientId
        }
  let !req = Message.Request.Request
        { header = hdr
        , body = inner
        }
  let enc = Message.Request.toChunks req
  ExceptT $ ChannelSig.send resource enc >>= \case
    Right (_ :: ()) -> pure (Right ())
    Left e -> pure (Left (Communicate $ CommunicationException key host thePort corrId (Send e)))
  rawSz <- ExceptT $ ChannelSig.receiveExactly resource 4 >>= \case
    Right rawSz -> pure (Right rawSz)
    Left e -> pure (Left (Communicate $ CommunicationException key host thePort corrId (Receive e)))
  let sz = BigEndian.indexByteArray rawSz 0 :: Int32
  when (sz < 0) (throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseLengthNegative)))
  -- Technically, there's nothing wrong with a response that is
  -- larger than 512MB. It's just not going to happen in practice.
  when (sz >= 512_000_000) (throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseLengthTooHigh)))
  byteArray <- ExceptT $ ChannelSig.receiveExactly resource (fromIntegral sz) >>= \case
    Right byteArray -> pure (Right byteArray)
    Left e -> pure (Left (Communicate $ CommunicationException key host thePort corrId (Receive e)))
  payload <- case respHeaderVersion of
    0 ->  case Parser.parseByteArray (Header.Response.V0.parser Ctx.Top) byteArray of
      Parser.Failure _ -> throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseHeaderMalformed))
      Parser.Success (Parser.Slice off len respHdr) -> if respHdr.correlationId == corrId
        then pure (Bytes byteArray off len)
        else throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseHeaderIncorrectCorrelationId))
    1 -> case Parser.parseByteArray (Header.Response.V1.parser Ctx.Top) byteArray of
      Parser.Failure _ -> throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseHeaderMalformed))
      Parser.Success (Parser.Slice off len respHdr) -> if respHdr.correlationId == corrId
        then pure (Bytes byteArray off len)
        else throwE (Communicate $ CommunicationException key host thePort corrId (Protocol Types.ResponseHeaderIncorrectCorrelationId))
    _ -> errorWithoutStackTrace "kafka-exchange: huge mistake, expecting a response header version other than 0 or 1"
  let !newEnv = Env host thePort resource (corrId + 1)
  let envs' = C.replaceAt envs envIx newEnv
  pure (Result envs' (Correlated corrId payload))

-- | Throw an exception, short-circuiting the rest of the
-- computation. This plumbs the exception around explicitly
-- and does not rely on stack unwinding like @throwIO@ and
-- @catch@.
throw ::
     KafkaException e -- ^ The exception to throw
  -> M e n a
throw e = M $ \_ _ _ -> pure (Left e)

throwProtocolException ::
     Fin n
  -> ApiKey -- ^ API Key of Request
  -> Int32 -- ^ Correlation ID
  -> ProtocolException -- ^ The exception to throw
  -> M e n a
throwProtocolException !fin !k !corrId e = do
  theHost <- lookupHostname fin
  thePort <- lookupPort fin
  throw
    $ Communicate
    $ CommunicationException k theHost thePort corrId
    $ Protocol e

throwErrorCode ::
     Fin n
  -> ApiKey -- ^ API Key of Request
  -> Int32 -- ^ Correlation ID
  -> Context
  -> ErrorCode -- ^ The exception to throw
  -> M e n a
throwErrorCode !fin !k !corrId ctx e = do
  theHost <- lookupHostname fin
  thePort <- lookupPort fin
  throw
    $ Communicate
    $ CommunicationException k theHost thePort corrId
    $ ErrorCode ctx e

substitute :: (m :=: n) -> M e m a -> M e n a
{-# inline substitute #-}
substitute _ (M x) = M x
