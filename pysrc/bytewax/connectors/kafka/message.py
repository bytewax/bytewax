"""KafkaMessage definition."""

from dataclasses import dataclass, replace
from typing import Generic, List, Optional, Tuple

from confluent_kafka import KafkaError

from ._types import K2, V2, K, V


@dataclass(frozen=True)
class KafkaMessage(Generic[K, V]):
    """Class that holds a message from kafka with metadata.

    Use `KafkaMessage.key` to get the key and `KafkaMessage.value` to get
    the value.

    Check `KafkaMessage.error` before using the message.

    Other fields: `topic`, `headers`, `latency` `offset`, `partition`, `timestamp`
    """

    key: K
    value: V

    error: Optional[KafkaError]
    topic: Optional[str]
    headers: List[Tuple[str, bytes]]
    latency: Optional[float]
    offset: Optional[int]
    partition: Optional[int]
    timestamp: Tuple[int, int]

    def with_error(self, error: KafkaError) -> "KafkaMessage[K, V]":
        """Returns a new instance of the message with the specified error."""
        return replace(self, error=error)

    def with_key(self, key: K2) -> "KafkaMessage[K2, V]":
        """Returns a new instance of the message with the specified key."""
        return KafkaMessage(
            key=key,
            value=self.value,
            error=self.error,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def with_value(self, value: V2) -> "KafkaMessage[K, V2]":
        """Returns a new instance of the message with the specified value."""
        return KafkaMessage(
            key=self.key,
            value=value,
            error=self.error,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )
