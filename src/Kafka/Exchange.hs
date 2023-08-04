{-# language DataKinds #-}
{-# language DuplicateRecordFields #-}
{-# language OverloadedRecordDot #-}

module Kafka.Exchange
  ( M
    -- * Standard Exchanges
  , produce
  , findCoordinator
    -- * Derived Exchanges
  , findCoordinatorSingleton
  ) where

import Chan (M,KafkaException(Communicate),CommunicationException(..),Description(Protocol))
import Arithmetic.Types (Fin)
import Kafka.Exchange.Types (ProtocolException(ResponseArityMismatch))
import Kafka.Exchange.Types (Correlated(Correlated))
import Data.Text (Text)

import qualified Chan
import qualified Produce
import qualified FindCoordinator
import qualified Data.Primitive as PM
import qualified Data.Primitive.Contiguous as Contiguous
import qualified Kafka.ApiKey as ApiKey
import qualified Request.FindCoordinator
import qualified Response.FindCoordinator

produce ::
     Fin n
  -> Produce.Request
  -> M e n Produce.Response
produce = Produce.exchange_

findCoordinator ::
     Fin n
  -> FindCoordinator.Request
  -> M e n FindCoordinator.Response
findCoordinator = FindCoordinator.exchange_

-- | This uses key type 0 for the consumer group key type. It does not
-- appear that any other key types are ever used. A find-coordinator
-- request only takes a key type and an array of consumer group names.
-- This populates the names array with a single name and expects that
-- the brokers responds with information about a single coordinator.
findCoordinatorSingleton ::
     Fin n
  -> Text -- ^ Consumer group name
  -> M e n Response.FindCoordinator.Coordinator
findCoordinatorSingleton fin cgname = do
  Correlated corrId resp1 <- FindCoordinator.exchange fin Request.FindCoordinator.Request
    { keyType = 0
    , coordinatorKeys = Contiguous.singleton cgname
    }
  if PM.sizeofSmallArray resp1.coordinators /= 1
    then Chan.throw $ Communicate $ CommunicationException
      ApiKey.FindCoordinator
      corrId
      (Chan.Protocol ResponseArityMismatch)
    else pure $! PM.indexSmallArray resp1.coordinators 0

