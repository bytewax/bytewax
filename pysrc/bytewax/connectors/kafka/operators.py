"""Operators for the kafka source and sink.

It's suggested to import operators like this:
>>> from bytewax.connectors.kafka import operators as kop

And then you can use the operators like this:
>>> kafka_input = kop.input("kafka_inp", flow, brokers=[...], topics=[...])
>>> kop.output("kafka-out", kafka_input.oks, brokers=[...], topic="...")
"""
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, TypeVar, Union, cast

from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka import KafkaError as ConfluentKafkaError

import bytewax.operators as op
from bytewax.connectors.kafka import (
    K2,
    V2,
    K,
    KafkaError,
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceError,
    KafkaSourceMessage,
    SerializedKafkaSinkMessage,
    SerializedKafkaSourceMessage,
    V,
)
from bytewax.connectors.kafka.serde import SchemaDeserializer, SchemaSerializer
from bytewax.dataflow import Dataflow, Stream, operator

X = TypeVar("X")
"""Type of sucessfully processed items."""

E = TypeVar("E")
"""Type of errors."""


@dataclass(frozen=True)
class KafkaOpOut(Generic[X, E]):
    """Result streams from Kafka operators."""

    oks: Stream[X]
    """Successfully processed items."""

    errs: Stream[E]
    """Errors."""


@operator
def _kafka_error_split(
    step_id: str, up: Stream[Union[KafkaSourceMessage[K2, V2], KafkaError[K, V]]]
) -> KafkaOpOut[KafkaSourceMessage[K2, V2], KafkaError[K, V]]:
    """Split the stream from KafkaSource between oks and errs."""
    branch = op.branch("branch", up, lambda msg: isinstance(msg, KafkaSourceMessage))
    # Cast the streams to the proper expected types.
    oks = cast(Stream[KafkaSourceMessage[K2, V2]], branch.trues)
    errs = cast(Stream[KafkaError[K, V]], branch.falses)
    return KafkaOpOut(oks, errs)


@operator
def _to_sink(
    step_id: str, up: Stream[Union[KafkaSourceMessage[K, V], KafkaSinkMessage[K, V]]]
) -> Stream[KafkaSinkMessage[K, V]]:
    """Automatically convert a KafkaSourceMessage to KafkaSinkMessage."""

    def shim_mapper(
        msg: Union[KafkaSourceMessage[K, V], KafkaSinkMessage[K, V]],
    ) -> KafkaSinkMessage[K, V]:
        if isinstance(msg, KafkaSourceMessage):
            return msg.to_sink()
        else:
            return msg

    return op.map("map", up, shim_mapper)


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
) -> KafkaOpOut[SerializedKafkaSourceMessage, KafkaSourceError]:
    """Consume from Kafka as an input source.

    Partitions are the unit of parallelism. Can support exactly-once
    processing.

    :arg step_id: Unique Id.

    :arg flow: Dataflow.

    :arg brokers: List of `host:port` strings of Kafka brokers.

    :arg topics: List of topics to consume from.

    :arg tail: Whether to wait for new data on this topic when the end
        is initially reached.

    :arg starting_offset: Can be either
        {py:obj}`confluent_kafka.OFFSET_BEGINNING` or
        {py:obj}`confluent_kafka.OFFSET_END`.

    :arg add_config: Any additional configuration properties. See the
        `rdkafka`
        [docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
        for options.

    :arg batch_size: How many messages to consume at most at each
        poll.

    :returns: A stream of consumed items and a stream of consumer
        errors.

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
    up: Stream[Union[SerializedKafkaSourceMessage, SerializedKafkaSinkMessage]],
    *,
    brokers: List[str],
    topic: str,
    add_config: Optional[Dict[str, str]] = None,
) -> None:
    """Produce to Kafka as an output sink.

    Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    :arg step_id: Unique ID.

    :arg up: Stream of fully serialized messages. Key and value must
        be {py:obj}`~bytewax.connectors.kafka.MaybeStrBytes`.

    :arg brokers: List of `host:port` strings of Kafka brokers.

    :arg topic: Topic to produce to. If individual items have
        {py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage.topic`
        set, will override this per-message.

    :arg add_config: Any additional configuration properties. See the
        `rdkafka`
        [docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
        for options.

    """
    return _to_sink("to_sink", up).then(
        op.output, "kafka_output", KafkaSink(brokers, topic, add_config)
    )


@operator
def deserialize_key(
    step_id: str,
    up: Stream[KafkaSourceMessage[K, V]],
    deserializer: SchemaDeserializer[K, K2],
) -> KafkaOpOut[KafkaSourceMessage[K2, V], KafkaError[K, V]]:
    """Deserialize Kafka message keys.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg deserializer: To use.

    :returns: Stream of deserialized messages and a stream of
        deserialization errors.

    """

    # Make sure the first KafkaMessage in the return type's Union represents
    # the `oks` stream and the second one the `errs` stream for the
    # `_kafka_error_split` operator.
    def shim_mapper(
        msg: KafkaSourceMessage[K, V],
    ) -> Union[KafkaSourceMessage[K2, V], KafkaError[K, V]]:
        try:
            key = deserializer.de(msg.key)
            return msg._with_key(key)
        except Exception as e:
            err = ConfluentKafkaError(ConfluentKafkaError._KEY_DESERIALIZATION, f"{e}")
            return KafkaError(err, msg)

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split")


@operator
def deserialize_value(
    step_id: str,
    up: Stream[KafkaSourceMessage[K, V]],
    deserializer: SchemaDeserializer[V, V2],
) -> KafkaOpOut[KafkaSourceMessage[K, V2], KafkaError[K, V]]:
    """Deserialize Kafka message values.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg deserializer: To use.

    :returns: Stream of deserialized messages and a stream of
        deserialization errors.

    """

    def shim_mapper(
        msg: KafkaSourceMessage[K, V],
    ) -> Union[KafkaSourceMessage[K, V2], KafkaError[K, V]]:
        try:
            value = deserializer.de(msg.value)
            return msg._with_value(value)
        except Exception as e:
            err = ConfluentKafkaError(
                ConfluentKafkaError._VALUE_DESERIALIZATION, f"{e}"
            )
            return KafkaError(err, msg)

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split_err")


@operator
def deserialize(
    step_id: str,
    up: Stream[KafkaSourceMessage[K, V]],
    *,
    key_deserializer: SchemaDeserializer[K, K2],
    val_deserializer: SchemaDeserializer[V, V2],
) -> KafkaOpOut[KafkaSourceMessage[K2, V2], KafkaError[K, V]]:
    """Deserialize Kafka messages.

    If there is an error on deserializing either key or value, the
    original message will be attached to the error.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg key_deserializer: To use.

    :arg val_deserializer: To use.

    :returns: Stream of deserialized messages and a stream of
        deserialization errors.

    """

    # Use a single map step rather than concatenating
    # deserialize_key and deserialize_value so we can
    # return the original message if any of the 2 fail.
    def shim_mapper(
        msg: KafkaSourceMessage[K, V],
    ) -> Union[KafkaSourceMessage[K2, V2], KafkaError[K, V]]:
        try:
            key = key_deserializer.de(msg.key)
        except Exception as e:
            err = ConfluentKafkaError(ConfluentKafkaError._KEY_DESERIALIZATION, f"{e}")
            return KafkaError(err, msg)

        try:
            value = val_deserializer.de(msg.value)
        except Exception as e:
            err = ConfluentKafkaError(
                ConfluentKafkaError._VALUE_DESERIALIZATION, f"{e}"
            )
            return KafkaError(err, msg)

        return msg._with_key_and_value(key, value)

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split_err")


@operator
def serialize_key(
    step_id: str,
    up: Stream[Union[KafkaSourceMessage[K, V], KafkaSinkMessage[K, V]]],
    serializer: SchemaSerializer[K, K2],
) -> Stream[KafkaSinkMessage[K2, V]]:
    """Serialize Kafka message keys.

    If there is an error on serializing, this operator will raise an
    exception.

    :arg step_id: Unique ID.

    :arg up: Stream. Will automatically convert source messages to
        sink messages via
        {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage.to_sink`.

    :arg serializer: To use.

    :returns: Stream of serialized messages.

    """

    def shim_mapper(msg: KafkaSinkMessage[K, V]) -> KafkaSinkMessage[K2, V]:
        key = serializer.ser(msg.key)
        return msg._with_key(key)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)


@operator
def serialize_value(
    step_id: str,
    up: Stream[Union[KafkaSourceMessage[K, V], KafkaSinkMessage[K, V]]],
    serializer: SchemaSerializer[V, V2],
) -> Stream[KafkaSinkMessage[K, V2]]:
    """Serialize Kafka message values.

    If there is an error on serializing, this operator will raise an
    exception.

    :arg step_id: Unique ID.

    :arg up: Stream. Will automatically convert source messages to
        sink messages via
        {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage.to_sink`.

    :arg serializer: To use.

    :returns: Stream of serialized messages.

    """

    def shim_mapper(msg: KafkaSinkMessage[K, V]) -> KafkaSinkMessage[K, V2]:
        value = serializer.ser(msg.value)
        return msg._with_value(value)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)


@operator
def serialize(
    step_id: str,
    up: Stream[Union[KafkaSourceMessage[K, V], KafkaSinkMessage[K, V]]],
    *,
    key_serializer: SchemaSerializer[K, K2],
    val_serializer: SchemaSerializer[V, V2],
) -> Stream[KafkaSinkMessage[K2, V2]]:
    """Serialize Kafka messages.

    If there is an error on serializing, this operator will raise an
    exception.

    :arg step_id: Unique ID.

    :arg up: Stream. Will automatically convert source messages to
        sink messages via
        {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage.to_sink`.

    :arg key_serializer: To use.

    :arg val_serializer: To use.

    :returns: Stream of serialized messages.

    """

    def shim_mapper(msg: KafkaSinkMessage[K, V]) -> KafkaSinkMessage[K2, V2]:
        key = key_serializer.ser(msg.key)
        value = val_serializer.ser(msg.value)
        return msg._with_key_and_value(key, value)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)
