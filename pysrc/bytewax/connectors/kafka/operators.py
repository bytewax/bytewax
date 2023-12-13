"""Operators for the kafka source and sink."""
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, Tuple, Union, cast

from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka.error import KafkaError

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import Stream, operator

from ._types import K2, V2, K, MaybeStrBytes, V
from .message import KafkaMessage
from .serde import SchemaDeserializer, SchemaSerializer
from .sink import KafkaSink
from .source import KafkaSource

__all__ = [
    "deserialize",
    "deserialize_key",
    "deserialize_value",
    "serialize",
    "serialize_key",
    "serialize_value",
    "input",
    "output",
]


@dataclass(frozen=True)
class KafkaStreams(Generic[K, V, K2, V2]):
    """Contains two streams of KafkaMessages, oks and errors.

    - KafkaStreams.oks:
        A stream of `KafkaMessage`s where `KafkaMessage.error is None`
    - KafkaStreams.errors:
        A stream of `KafkaMessage`s where `KafkaMessage.error is not None`
    """

    oks: Stream[KafkaMessage[K, V]]
    errs: Stream[KafkaMessage[K2, V2]]


@operator
def _kafka_error_split(
    step_id: str, up: Stream[Union[KafkaMessage[K, V], KafkaMessage[K2, V2]]]
) -> KafkaStreams[K, V, K2, V2]:
    """Split a stream of KafkaMessages in two."""

    def predicate(msg: KafkaMessage) -> bool:
        return msg.error is None

    branch = op.branch("branch", up, predicate)
    # Cast the streams to the proper expected types.
    # This assumes that messages where error is None will be KafkaMessage[K, V]
    # while messages where error is not None will be KafkaMessage[K2, V2]
    oks = cast(Stream[KafkaMessage[K, V]], branch.trues)
    errs = cast(Stream[KafkaMessage[K2, V2]], branch.falses)
    return KafkaStreams(oks, errs)


_default_add_config: Dict = {}


@operator
def input(  # noqa A001
    step_id: str,
    flow: Dataflow,
    *,
    brokers: List[str],
    topics: List[str],
    tail: bool = True,
    starting_offset: int = OFFSET_BEGINNING,
    add_config: Optional[Dict[str, str]] = None,
    batch_size: int = 1000,
) -> KafkaStreams[MaybeStrBytes, MaybeStrBytes, MaybeStrBytes, MaybeStrBytes]:
    """Use a set of Kafka topics as an input source.

    Partitions are the unit of parallelism.
    Can support exactly-once processing.

    Messages are emitted into the dataflow
    as `bytewax.connectors.kafka.KafkaMessage` objects.

    Args:
        step_id:
            Unique name for this step
        flow:
            A Dataflow
        brokers:
            List of `host:port` strings of Kafka brokers.
        topics:
            List of topics to consume from.
        tail:
            Whether to wait for new data on this topic when the
            end is initially reached.
        starting_offset:
            Can be either `confluent_kafka.OFFSET_BEGINNING` or
            `confluent_kafka.OFFSET_END`. Defaults to beginning of
            topic.
        add_config:
            Any additional configuration properties. See the `rdkafka`
            [docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.
        batch_size:
            How many messages to consume at most at each poll. Defaults to 1000.
    """
    return op.input(
        "kafka_input",
        flow,
        KafkaSource(
            brokers,
            topics,
            tail,
            starting_offset,
            add_config,
            batch_size,
            # Don't raise on errors, since we split
            # the stream and let users handle that
            raise_on_errors=False,
        ),
    ).then(_kafka_error_split, "split_err")


@operator
def output(
    step_id: str,
    up: Stream[Tuple[bytes, bytes]],
    *,
    brokers: List[str],
    topic: str,
    add_config: Optional[Dict[str, str]] = None,
) -> None:
    """Use a single Kafka topic as an output sink.

    Items consumed from the dataflow must look like two-tuples of
    `(key_bytes, value_bytes)`. Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    Args:
        step_id:
            Unique name for this step
        up:
            A stream of `(key_bytes, value_bytes)` 2-tuples
        brokers:
            List of `host:port` strings of Kafka brokers.
        topic:
            Topic to produce to.
        add_config:
            Any additional configuration properties. See the `rdkafka`
            [docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.
    """
    return op.output("kafka_output", up, KafkaSink(brokers, topic, add_config))


@operator
def deserialize_key(
    step_id: str,
    up: Stream[KafkaMessage[K, V]],
    deserializer: SchemaDeserializer[K, K2],
) -> KafkaStreams[K2, V, K, V]:
    """Deserialize the key of a KafkaMessage using the provided SchemaDeserializer.

    Returns an object with two attributes:
        - .oks: A stream of `KafkaMessage`s where `KafkaMessage.error is None`
        - .errors: A stream of `KafkaMessage`s where `KafkaMessage.error is not None`
    """

    # Make sure the first KafkaMessage in the return type's Union represents
    # the `oks` stream and the second one the `errs` stream for the
    # `_kafka_error_split` operator.
    def shim_mapper(
        msg: KafkaMessage[K, V]
    ) -> Union[KafkaMessage[K2, V], KafkaMessage[K, V]]:
        try:
            key = deserializer.de(msg.key)
            return msg.with_key(key)
        except Exception as e:
            return msg.with_error(KafkaError(KafkaError._KEY_DESERIALIZATION, f"{e}"))

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split")


@operator
def deserialize_value(
    step_id: str,
    up: Stream[KafkaMessage[K, V]],
    deserializer: SchemaDeserializer[V, V2],
) -> KafkaStreams[K, V2, K, V]:
    """Deserialize the value of a KafkaMessage using the provided SchemaDeserializer.

    Returns an object with two attributes:
        - .oks: A stream of `KafkaMessage`s where `KafkaMessage.error is None`
        - .errors: A stream of `KafkaMessage`s where `KafkaMessage.error is not None`
    """

    # Make sure the first KafkaMessage in the return type's Union represents
    # the `oks` stream and the second one the `errs` stream for the
    # `_kafka_error_split` operator.
    def shim_mapper(
        msg: KafkaMessage[K, V]
    ) -> Union[KafkaMessage[K, V2], KafkaMessage[K, V]]:
        try:
            value = deserializer.de(msg.value)
            return msg.with_value(value)
        except Exception as e:
            return msg.with_error(KafkaError(KafkaError._VALUE_DESERIALIZATION, f"{e}"))

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split_err")


@operator
def deserialize(
    step_id: str,
    up: Stream[KafkaMessage[K, V]],
    *,
    key_deserializer: SchemaDeserializer[K, K2],
    val_deserializer: SchemaDeserializer[V, V2],
) -> KafkaStreams[K2, V2, Union[K, K2], Union[V, V2]]:
    """Serialize both keys and values with the given serializers.

    Returns an object with two attributes:
        - .oks: A stream of `KafkaMessage`s where `KafkaMessage.error is None`
        - .errors: A stream of `KafkaMessage`s where `KafkaMessage.error is not None`
    A message will be put in .errors even if only one of the deserializers fail.
    """
    keys = deserialize_key("de-key", up, key_deserializer)
    values = deserialize_value("de-val", keys.oks, val_deserializer)
    errors = cast(
        # Either of key and value deserialization might have failed in the
        # errors stream. Since we don't know better, just Union the possible
        # output types
        Stream[KafkaMessage[Union[K, K2], Union[V, V2]]],
        op.merge("merg-errors", keys.errs, values.errs),
    )
    return KafkaStreams(values.oks, errors)


@operator
def serialize_key(
    step_id: str, up: Stream[Tuple[K, V]], serializer: SchemaSerializer[K, K2]
) -> Stream[Tuple[K2, V]]:
    """Serialize the key of a KafkaMessage using the provided SchemaSerializer.

    Crash if any error occurs.
    """

    def shim_mapper(key_msg: Tuple[K, V]) -> Tuple[K2, V]:
        key, value = key_msg
        return (serializer.ser(key), value)

    return op.map("map", up, shim_mapper)


@operator
def serialize_value(
    step_id: str, up: Stream[Tuple[K, V]], serializer: SchemaSerializer[V, V2]
) -> Stream[Tuple[K, V2]]:
    """Serialize the value of a KafkaMessage using the provided SchemaSerializer.

    Crash if any error occurs.
    """

    def shim_mapper(key_msg: Tuple[K, V]) -> Tuple[K, V2]:
        key, value = key_msg
        return (key, serializer.ser(value))

    return op.map("map", up, shim_mapper)


@operator
def serialize(
    step_id: str,
    up: Stream[Tuple[K, V]],
    *,
    key_serializer: SchemaSerializer[K, K2],
    val_serializer: SchemaSerializer[V, V2],
) -> Stream[Tuple[K2, V2]]:
    """Serialize both keys and values with the given serializers.

    Crash if any error occurs.
    """
    ser_keys = serialize_key("ser-key", up, key_serializer)
    return serialize_value("val-ser", ser_keys, val_serializer)
