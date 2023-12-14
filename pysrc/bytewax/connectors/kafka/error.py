"""Error handling related stuff."""

from dataclasses import dataclass
from typing import Generic

from confluent_kafka import KafkaError as ConfluentKafkaError

from ._types import K, V
from .message import KafkaSourceMessage


@dataclass(frozen=True)
class KafkaError(Generic[K, V]):
    """Custom KafkaErorr class that holds both the exception and the message itself."""

    err: ConfluentKafkaError
    msg: KafkaSourceMessage[K, V]
