"""KafkaMessage definition."""

from dataclasses import dataclass, field
from typing import Generic, List, Optional, Tuple, cast

from ._types import K2, V2, K, V


@dataclass
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
        """Modifies the key in place and returns the instance."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        # But we can modify the message in place and return it
        this = cast(KafkaSourceMessage[K2, V], self)
        this.key = key
        return this

    def _with_value(self, value: V2) -> "KafkaSourceMessage[K, V2]":
        """Modifies the value in place and returns the instance."""
        this = cast(KafkaSourceMessage[K, V2], self)
        this.value = value
        return this

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSourceMessage[K2, V2]":
        """Modifies both key and value in place and returns the instance."""
        this = cast(KafkaSourceMessage[K2, V2], self)
        this.key = key
        this.value = value
        return this


@dataclass
class KafkaSinkMessage(Generic[K, V]):
    """Class that holds a message from kafka with metadata.

    Use `KafkaMessage.key` to get the key and `KafkaMessage.value` to get
    the value.

    Other fields: `topic`, `headers`, `partition`, `timestamp`
    """

    key: K
    value: V

    topic: Optional[str] = field(default=None)
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    partition: Optional[int] = field(default=None)
    timestamp: Optional[Tuple[int, int]] = field(default=None)

    def _with_key(self, key: K2) -> "KafkaSinkMessage[K2, V]":
        """Modifies the key in place and returns the instance."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        # But we can modify the message in place and return it
        this = cast(KafkaSinkMessage[K2, V], self)
        this.key = key
        return this

    def _with_value(self, value: V2) -> "KafkaSinkMessage[K, V2]":
        """Modifies the value in place and returns the instance."""
        this = cast(KafkaSinkMessage[K, V2], self)
        this.value = value
        return this

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSinkMessage[K2, V2]":
        """Modifies both key and value in place and returns the instance."""
        this = cast(KafkaSinkMessage[K2, V2], self)
        this.key = key
        this.value = value
        return this
