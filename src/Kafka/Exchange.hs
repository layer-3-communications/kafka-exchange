{-# language BangPatterns #-}
{-# language MagicHash #-}
{-# language DataKinds #-}
{-# language PatternSynonyms #-}
{-# language DuplicateRecordFields #-}
{-# language OverloadedRecordDot #-}
{-# language UnboxedTuples #-}

module Kafka.Exchange
  ( M
  , Env
  , Broker(..)
  , KafkaException(..)
  , CommunicationException(..)
  , ProtocolException(..)
  , Description(..)
  , openEnvironment
  , run
  , runWithEnv
  , with
  , throw
  , throwErrorCode
  , throwProtocolException
  , lift
  , substitute
    -- * Standard Exchanges
  , produce
  , findCoordinator
  , initProducerId
  , metadata
  , fetch
  , fetchV12
  , listOffsets
    -- * Derived Exchanges
  , findCoordinatorSingleton
  , initProducerIdNontransactional
  , metadataAll
  , metadataNone
  , metadataOne
  , metadataOneAutoCreate
  , produceSingleton
  , bootstrap
  , bootstrapOne
  , listOffsetsOnePartition
  ) where

import Chan (M,KafkaException(..),CommunicationException(..),Description(..),run,runWithEnv,with,throw,lift,substitute,throwProtocolException,throwErrorCode)
import Chan (Env,openEnvironment)
import Arithmetic.Types (Fin(Fin),Fin#)
import Arithmetic.Nat (pattern N0#)
import Kafka.Exchange.Types (ProtocolException(..))
import Kafka.Exchange.Types (Correlated(Correlated),Broker(..))
import Data.Text (Text)
import Data.Int (Int32)
import Data.Primitive (SmallArray)
import Kafka.Parser.Context (Context(Top,Field,Index))
import Kafka.Parser.Context (ContextualizedErrorCode(..))
import Kafka.ErrorCode (pattern None)
import Kafka.Acknowledgments (Acknowledgments)

import qualified ChannelSig
import qualified Arithmetic.Fin as Fin
import qualified Arithmetic.Lt as Lt
import qualified Arithmetic.Nat as Nat
import qualified Kafka.Parser.Context as Ctx
import qualified Fetch
import qualified FetchV12
import qualified Produce
import qualified FindCoordinator
import qualified InitProducerId
import qualified Metadata
import qualified ListOffsets
import qualified Data.Primitive as PM
import qualified Data.Primitive.Contiguous as Contiguous
import qualified Kafka.ApiKey as ApiKey
import qualified Request.InitProducerId
import qualified Response.InitProducerId
import qualified Request.FindCoordinator
import qualified Response.FindCoordinator
import qualified Request.Metadata
import qualified Response.Metadata
import qualified Request.Produce
import qualified Response.Produce
import qualified Request.Fetch
import qualified Response.Fetch
import qualified Request.ListOffsets
import qualified Response.ListOffsets
import qualified Kafka.RecordBatch.Request

produce ::
     Fin# n
  -> Produce.Request
  -> M e n Produce.Response
produce = Produce.exchange_

initProducerId ::
     Fin# n
  -> InitProducerId.Request
  -> M e n InitProducerId.Response
initProducerId = InitProducerId.exchange_

findCoordinator ::
     Fin# n
  -> FindCoordinator.Request
  -> M e n FindCoordinator.Response
findCoordinator = FindCoordinator.exchange_

metadata ::
     Fin# n
  -> Metadata.Request
  -> M e n Metadata.Response
metadata = Metadata.exchange_

fetch ::
     Fin# n
  -> Fetch.Request
  -> M e n Fetch.Response
fetch = Fetch.exchange_

fetchV12 ::
     Fin# n
  -> FetchV12.Request
  -> M e n FetchV12.Response
fetchV12 = FetchV12.exchange_

listOffsets ::
     Fin# n
  -> ListOffsets.Request
  -> M e n ListOffsets.Response
listOffsets = ListOffsets.exchange_

-- | This uses key type 0 for the consumer group key type. It does not
-- appear that any other key types are ever used. A find-coordinator
-- request only takes a key type and an array of consumer group names.
-- This populates the names array with a single name and expects that
-- the brokers responds with information about a single coordinator.
--
-- This checks that no error code is associated with the coordinator.
findCoordinatorSingleton ::
     Fin# n
  -> Text -- ^ Consumer group name
  -> M e n Response.FindCoordinator.Coordinator
findCoordinatorSingleton fin cgname = do
  Correlated corrId resp1 <- FindCoordinator.exchange fin Request.FindCoordinator.Request
    { keyType = 0
    , coordinatorKeys = Contiguous.singleton cgname
    }
  if PM.sizeofSmallArray resp1.coordinators /= 1
    then throwProtocolException fin ApiKey.FindCoordinator corrId ResponseArityMismatch
    else do
      let coord = PM.indexSmallArray resp1.coordinators 0
       in case coord.errorCode of 
            None -> pure coord
            e -> throwErrorCode
              fin ApiKey.FindCoordinator corrId
              (Index 0 (Field Ctx.Coordinators Top)) e

listOffsetsOnePartition ::
     Fin# n
  -> Text -- topic name
  -> Request.ListOffsets.Partition -- partition
  -> M e n Response.ListOffsets.Partition
listOffsetsOnePartition !fin !topicName reqPrt = do
  let req = Request.ListOffsets.Request (-1) 0 $! Contiguous.singleton $! Request.ListOffsets.Topic topicName $! Contiguous.singleton reqPrt
  Correlated corrId resp <- ListOffsets.exchange fin req
  if PM.sizeofSmallArray resp.topics /= 1
    then throwProtocolException fin ApiKey.ListOffsets corrId ResponseArityMismatch
    else do
      let t = PM.indexSmallArray resp.topics 0
      if PM.sizeofSmallArray t.partitions /= 1
        then throwProtocolException fin ApiKey.ListOffsets corrId ResponseArityMismatch
        else do
          let respPrt = PM.indexSmallArray t.partitions 0
          case respPrt.errorCode of
            None -> pure respPrt
            e -> throwErrorCode
              fin ApiKey.ListOffsets corrId
              (Index 0 (Field Ctx.Partitions (Index 0 (Field Ctx.Topics Top)))) e

-- | Builds a produce request for a single partition, issues the request,
-- parses the response, asserts that the response contains exactly one
-- topic and exactly one partition within that topic, and returns the
-- information about this partition. This does not check the error code
-- in the partition because Kafka reports errors through several different
-- fields. The user would lose information about the failure if we attempted
-- to handle it here.
produceSingleton ::
     Fin# n
  -> Acknowledgments -- ^ Acknowledgements
  -> Int32 -- ^ Timeout milliseconds
  -> Text -- ^ Topic name
  -> Int32 -- ^ Partition index
  -> Kafka.RecordBatch.Request.RecordBatch -- ^ Records
  -> M e n Response.Produce.Partition
produceSingleton !fin !acks !timeoutMs !topicName !partitionIx records = do
  let req = Request.Produce.singleton acks timeoutMs topicName partitionIx records
  Correlated corrId resp <- Produce.exchange fin req
  if PM.sizeofSmallArray resp.topics /= 1
    then throwProtocolException fin ApiKey.Produce corrId ResponseArityMismatch
    else do
      let topic = PM.indexSmallArray resp.topics 0
       in if PM.sizeofSmallArray topic.partitions /= 1
            then throwProtocolException fin ApiKey.Produce corrId ResponseArityMismatch
            else do
              let !(# partition #) = PM.indexSmallArray## topic.partitions 0
               in pure partition

-- | Requests a producer id for a nontransactional producer. Checks the
-- error code, so you don't need to recheck it after calling this function.
initProducerIdNontransactional ::
     Fin# n 
  -> M e n InitProducerId.Response
initProducerIdNontransactional fin = do
  Correlated corrId resp <- InitProducerId.exchange fin
    Request.InitProducerId.request
  case resp.errorCode of 
    None -> pure resp
    e -> throwErrorCode fin ApiKey.InitProducerId corrId Top e

-- | Discover all brokers and all topics in the cluster.
-- Checks the error codes on all topics and partitions.
metadataAll :: Fin# n -> M e n Metadata.Response
metadataAll fin = do
  Correlated corrId resp <- Metadata.exchange fin Request.Metadata.all
  case Response.Metadata.findErrorCode resp of
    Just (ContextualizedErrorCode ctx e) -> do
      throwErrorCode fin ApiKey.Metadata corrId ctx e
    Nothing -> pure resp

-- | Discover all brokers in the cluster. Does not request any information
-- about topics. This does not check any error codes because the only error
-- codes in a metadata response are associated with topics and partitions.
metadataNone :: Fin# n -> M e n Metadata.Response
metadataNone fin = Metadata.exchange_ fin Request.Metadata.none

-- Shared by both the auto-create and the no-auto-create variant.
finishMetadataOne :: Fin# n -> Int32 -> Response.Metadata.Response -> M e n Response.Metadata.Topic
finishMetadataOne fin !corrId resp = case Response.Metadata.findErrorCode resp of
  Just (ContextualizedErrorCode ctx e) ->
    throwErrorCode fin ApiKey.Metadata corrId ctx e
  Nothing -> case PM.sizeofSmallArray resp.topics of
    1 -> do
      let topic = PM.indexSmallArray resp.topics 0
      pure topic
    _ -> Chan.throwProtocolException fin ApiKey.Produce corrId ResponseArityMismatch

-- | Variant of 'metadataOneAutoCreate' that does not auto create the topic.
metadataOne :: Fin# n -> Request.Metadata.Topic -> M e n Response.Metadata.Topic
metadataOne fin !topicName = do
  Correlated corrId resp <- Metadata.exchange fin Request.Metadata.Request
    { topics = Just $! Contiguous.singleton topicName
    , allowAutoTopicCreation = False
    , includeTopicAuthorizedOperations = False
    }
  finishMetadataOne fin corrId resp

-- | Inspect a single topic by name or by UUID with a metadata request.
-- Topic auto creation is enabled. Only returns the information about
-- the single topic, discarding information about the brokers.
metadataOneAutoCreate :: Fin# n -> Request.Metadata.Topic -> M e n Response.Metadata.Topic
metadataOneAutoCreate fin !topicName = do
  Correlated corrId resp <- Metadata.exchange fin Request.Metadata.Request
    { topics = Just $! Contiguous.singleton topicName
    , allowAutoTopicCreation = True
    , includeTopicAuthorizedOperations = False
    }
  finishMetadataOne fin corrId resp

-- | Call 'metadataAll' on each broker in the list until
-- a broker responds. Operates in the base monad and does
-- not persist any connections.
--
-- Returns metadata about all partitions of all topics.
bootstrap :: 
     Text -- ^ Client id
  -> SmallArray Broker
  -> ChannelSig.M (Either () Metadata.Response)
bootstrap !clientId !brokers = go 0 where
  go !ix = if ix < PM.sizeofSmallArray brokers
    then do
      e <- run clientId $ with (PM.indexSmallArray brokers ix) $ do
        metadataAll (Fin.greatest# N0#)
      case e of
        Left{} -> go (ix + 1)
        Right r -> pure (Right r)
    else pure (Left ())

-- | Variant of bootstrap that asks about a single topic in the @Metadata@
-- request. Does not use auto create. Extracts the sole topic from the response,
-- failing if there is not exactly one topic in the response. Returns the
-- full response as well as the one topic.
bootstrapOne ::
     Text -- ^ Client id
  -> Request.Metadata.Topic -- ^ Topic name
  -> SmallArray Broker
  -> ChannelSig.M (Either (KafkaException ()) (Response.Metadata.Response, Response.Metadata.Topic))
bootstrapOne !clientId !topicName !brokers = go 0 where
  go !ix = if ix < PM.sizeofSmallArray brokers
    then do
      e <- run clientId $ with (PM.indexSmallArray brokers ix) $ do
        Correlated corrId resp <- Metadata.exchange (Fin.greatest# Nat.N0#) Request.Metadata.Request
          { topics = Just $! Contiguous.singleton topicName
          , allowAutoTopicCreation = False
          , includeTopicAuthorizedOperations = False
          }
        soleTopic <- finishMetadataOne (Fin.greatest# N0#) corrId resp
        pure (resp,soleTopic)
      case e of
        Left e' -> if ix + 1 == PM.sizeofSmallArray brokers
          then pure (Left e')
          else go (ix + 1)
        Right r -> pure (Right r)
    else pure (Left (Application ()))
