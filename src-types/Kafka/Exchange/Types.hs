module Kafka.Exchange.Types
  ( ProtocolException(..)
  , Correlated(..)
  , Broker(..)
  ) where

import Data.Word (Word16)
import Data.Int (Int32)
import Data.Text (Text)
import Kafka.Parser.Context (Context)

-- | The minimal information needed to connect to a broker.
data Broker = Broker
  { host :: !Text
  , port :: !Word16
  } deriving (Eq,Ord)

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
  | ResponseBodyMalformedAt Context
  | ResponseArityMismatch
    -- ^ If we request one item and get back two items, this is the way
    -- that we communicate what happened.
  deriving (Show)
