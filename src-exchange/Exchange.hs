{-# language BangPatterns #-}
{-# language DeriveFunctor #-}
{-# language DerivingStrategies #-}
{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language NumericUnderscores #-}
{-# language OverloadedRecordDot #-}
{-# language ScopedTypeVariables #-}

module Exchange
  ( Request
  , Response
  , exchange
  , exchange_
  ) where

import Communication (Request,Response)
import Channel (M,KafkaException(..),Description(..),CommunicationException(..),lookupHostname,lookupPort)
import Arithmetic.Types (Fin#)
import Kafka.Exchange.Types (Correlated(Correlated))

import qualified Channel
import qualified Communication
import qualified Kafka.Exchange.Types as Types

exchange ::
     Fin# n -- which connection to use
  -> Request
  -> M e n (Correlated Response)
exchange fin inner = do
  Correlated corrId payload <- Channel.exchangeBytes fin Communication.apiKey
    Communication.apiVersion Communication.responseHeaderVersion (Communication.toChunks inner)
  case Communication.decode payload of
    Left ctx -> do
      host <- lookupHostname fin
      thePort <- lookupPort fin
      Channel.throw (Communicate (CommunicationException Communication.apiKey host thePort corrId (Protocol (Types.ResponseBodyMalformedAt ctx))))
    Right r -> pure (Correlated corrId r)

-- | Variant of exchange that discards the correlation id.
exchange_ ::
     Fin# n -- which connection to use
  -> Request
  -> M e n Response
exchange_ fin inner = fmap (\(Correlated _ v) -> v) (exchange fin inner)
