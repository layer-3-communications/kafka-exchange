module Kafka.Exchange.Types
  ( ProtocolException(..)
  , Correlated(..)
  ) where

import Data.Int (Int32)

-- | Associate a value with a correlation id.
data Correlated a = Correlated
  { correlationId :: !Int32
  , value :: !a
  }

data ProtocolException
  = ResponseLengthNegative
  | ResponseLengthTooHigh
  | ResponseHeaderMalformed
  | ResponseHeaderIncorrectCorrelationId
  | ResponseBodyMalformed
  | ResponseArityMismatch
    -- ^ If we request one item and get back two items, this is the way
    -- that we communicate what happened.
