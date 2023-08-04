{-# language BangPatterns #-}
{-# language DeriveFunctor #-}
{-# language DerivingStrategies #-}
{-# language ScopedTypeVariables #-}
{-# language LambdaCase #-}
{-# language NumericUnderscores #-}
{-# language DuplicateRecordFields #-}
{-# language OverloadedRecordDot #-}

module Exchange
  ( Request
  , Response
  , exchange
  , exchange_
  ) where

import Communication (Request,Response)
import Channel (M,KafkaException(..),Description(..),CommunicationException(..))
import Arithmetic.Types (Fin)
import Kafka.Exchange.Types (Correlated(Correlated))

import qualified Channel
import qualified Communication
import qualified Kafka.Exchange.Types as Types

exchange ::
     Fin n -- which connection to use
  -> Request
  -> M e n (Correlated Response)
exchange fin inner = do
  Correlated corrId payload <- Channel.exchangeBytes fin Communication.apiKey
    Communication.apiVersion Communication.responseHeaderVersion (Communication.toChunks inner)
  case Communication.decode payload of
    Left _ -> Channel.throw (Communicate (CommunicationException Communication.apiKey corrId (Protocol Types.ResponseBodyMalformed)))
    Right r -> pure (Correlated corrId r)

-- | Variant of exchange that discards the correlation id.
exchange_ ::
     Fin n -- which connection to use
  -> Request
  -> M e n Response
exchange_ fin inner = fmap (\(Correlated _ v) -> v) (exchange fin inner)
