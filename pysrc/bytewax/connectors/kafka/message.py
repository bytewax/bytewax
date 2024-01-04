"""KafkaMessage definition."""

from dataclasses import dataclass, field
from typing import Generic, List, Optional, Tuple

from ._types import K2, V2, K, K_co, V, V_co


@dataclass(frozen=True)
class KafkaSourceMessage(Generic[K, V]):
    """Class that holds a message from kafka with metadata.

    Use `msg.key` to get the key and `msg.value` to get
    the value.

    Other fields: `topic`, `headers`, `latency` `offset`, `partition`, `timestamp`
    """

    key: K
    value: V

    topic: Optional[str] = field(default=None)
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    latency: Optional[float] = field(default=None)
    offset: Optional[int] = field(default=None)
    partition: Optional[int] = field(default=None)
    timestamp: Optional[Tuple[int, int]] = field(default=None)

    def to_sink(self) -> "KafkaSinkMessage[K, V]":
        """Safely convert KafkaSourceMessage to KafkaSinkMessage.

        Only `key`, `value` and `timestamp` are used.
        """
        return KafkaSinkMessage(key=self.key, value=self.value, headers=self.headers)

    def _with_key(self, key: K2) -> "KafkaSourceMessage[K2, V]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSourceMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSourceMessage[K, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSourceMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSourceMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSourceMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )


@dataclass(frozen=True)
class KafkaSinkMessage(Generic[K_co, V_co]):
    """Class that holds a message from kafka with metadata.

    Use `KafkaMessage.key` to get the key and `KafkaMessage.value` to get
    the value.

    Other fields: `topic`, `headers`, `partition`, `timestamp`
    """

    key: K_co
    value: V_co

    topic: Optional[str] = None
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    partition: Optional[int] = None
    timestamp: int = 0

    def _with_key(self, key: K2) -> "KafkaSinkMessage[K2, V_co]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSinkMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSinkMessage[K_co, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSinkMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSinkMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSinkMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )
