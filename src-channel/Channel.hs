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
  , throw
  , exchangeBytes
    -- * Exceptions
  , KafkaException(..)
  , CommunicationException(..)
  , Description(..)
  ) where

import ChannelSig (Resource,SendException,ReceiveException,ConnectException)

import GHC.TypeNats (Nat,type (+))
import Data.Primitive (SmallArray,PrimArray)
import Data.Int (Int32,Int16)
import Data.Bytes.Types (Bytes(Bytes))
import Control.Monad (when)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT(ExceptT),runExceptT,throwE)
import Kafka.Exchange.Types (ProtocolException,Correlated(Correlated))
import Kafka.ApiKey (ApiKey)
import Data.Text (Text)
import Arithmetic.Types (Fin(Fin))
import Data.Primitive (ByteArray)
import Data.Bytes.Chunks (Chunks)

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
  = Connect !ConnectException
  | Communicate !CommunicationException
  | Application !e

data CommunicationException = CommunicationException
  { apiKey :: !ApiKey
  , correlationId :: !Int32
  , description :: !Description
  }

data Description
  = Send !SendException
  | Receive !ReceiveException
  | Protocol !ProtocolException

data Result a = Result
  !(SmallArray Env) -- next correlation ids. hostnames and sockets should not change
  !a
  deriving stock (Functor)

data Env = Env
  Text -- hostname
  Resource -- probably a socket
  Int32 -- correlation id

data M e (n :: Nat) a = M
  (    PrimArray Int -- Length m. Indices (>=0, <n) into environment array
    -> SmallArray Env -- Length n
    -> Text -- client id, shared by all sessions
    -> ChannelSig.M (Either (KafkaException e) (Result a))
  )
  deriving stock (Functor)

instance Applicative (M e n)
instance Monad (M e n)

with :: Text -> M e (n + 1) a -> M e n a
with host (M f) = M $ \ixs envs clientId -> case C.findIndex (\(Env existingHost _ _) -> existingHost == host) envs of
  Nothing -> ChannelSig.withConnection host $ \r -> case r of
    Left e -> pure (Left (Connect e))
    Right resource ->
      let !env = Env host resource 0
          !envs' = C.insertAt envs (C.size envs) env
          !ixs' = C.insertAt ixs (C.size ixs) (C.size envs)
       in f ixs' envs' clientId
  Just ix ->
    let ixs' = C.insertAt ixs (C.size ixs) ix
     in f ixs' envs clientId

-- connect hostname >>= \case
--   Left e -> pure (Left (KafkaException

run ::
     Text -- client id
  -> M e 0 a -- context with zero connections 
  -> ChannelSig.M (Either (KafkaException e) a)
run clientId (M f) = f mempty mempty clientId >>= \case
  Left e -> pure (Left e)
  Right (Result _ a) -> pure (Right a)

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
  let Env host resource corrId = PM.indexSmallArray envs envIx
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
    Left e -> pure (Left (Communicate $ CommunicationException key corrId (Send e)))
  rawSz <- ExceptT $ ChannelSig.receiveExactly resource 4 >>= \case
    Right rawSz -> pure (Right rawSz)
    Left e -> pure (Left (Communicate $ CommunicationException key corrId (Receive e)))
  let sz = BigEndian.indexByteArray rawSz 0 :: Int32
  when (sz < 0) (throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseLengthNegative)))
  -- Technically, there's nothing wrong with a response that is
  -- larger than 512MB. It's just not going to happen in practice.
  when (sz >= 512_000_000) (throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseLengthTooHigh)))
  byteArray <- ExceptT $ ChannelSig.receiveExactly resource (fromIntegral sz) >>= \case
    Right byteArray -> pure (Right byteArray)
    Left e -> pure (Left (Communicate $ CommunicationException key corrId (Receive e)))
  payload <- case respHeaderVersion of
    0 ->  case Parser.parseByteArray (Header.Response.V0.parser Ctx.Top) byteArray of
      Parser.Failure _ -> throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseHeaderMalformed))
      Parser.Success (Parser.Slice off len respHdr) -> if respHdr.correlationId == corrId
        then pure (Bytes byteArray off len)
        else throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseHeaderIncorrectCorrelationId))
    1 -> case Parser.parseByteArray (Header.Response.V1.parser Ctx.Top) byteArray of
      Parser.Failure _ -> throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseHeaderMalformed))
      Parser.Success (Parser.Slice off len respHdr) -> if respHdr.correlationId == corrId
        then pure (Bytes byteArray off len)
        else throwE (Communicate $ CommunicationException key corrId (Protocol Types.ResponseHeaderIncorrectCorrelationId))
    _ -> errorWithoutStackTrace "kafka-exchange: huge mistake, expecting a response header version other than 0 or 1"
  let !newEnv = Env host resource (corrId + 1)
  let envs' = C.replaceAt envs envIx newEnv
  pure (Result envs' (Correlated corrId payload))

throw :: KafkaException e -> M e n a
throw e = M $ \_ _ _ -> pure (Left e)
