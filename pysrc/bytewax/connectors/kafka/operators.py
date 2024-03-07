"""Operators for the kafka source and sink.

It's suggested to import operators like this:

```python
from bytewax.connectors.kafka import operators as kop
```

And then you can use the operators like this:

```python
from bytewax.dataflow import Dataflow
flow = Dataflow("kafka-in-out")
kafka_input = kop.input("kafka_inp", flow, brokers=[...], topics=[...])
kop.output("kafka-out", kafka_input.oks, brokers=[...], topic="...")
```
"""
from dataclasses import dataclass
from typing import Dict, Generic, List, Optional, TypeVar, Union, cast

import confluent_kafka
from confluent_kafka import OFFSET_BEGINNING
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.serialization import MessageField, SerializationContext

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
    MaybeStrBytes,
    SerializedKafkaSinkMessage,
    SerializedKafkaSourceMessage,
    V,
)
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
    up: Stream[KafkaSourceMessage[MaybeStrBytes, V]],
    deserializer: confluent_kafka.serialization.Deserializer,
) -> KafkaOpOut[KafkaSourceMessage[dict, V], KafkaError[MaybeStrBytes, V]]:
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
        msg: KafkaSourceMessage[MaybeStrBytes, V],
    ) -> Union[KafkaSourceMessage[dict, V], KafkaError[MaybeStrBytes, V]]:
        try:
            key = deserializer(
                msg.key, SerializationContext(topic=msg.topic, field=MessageField.KEY)
            )
            return msg._with_key(key)
        except Exception as e:
            err = ConfluentKafkaError(ConfluentKafkaError._KEY_DESERIALIZATION, f"{e}")
            return KafkaError(err, msg)

    return op.map("map", up, shim_mapper).then(_kafka_error_split, "split")


@operator
def deserialize_value(
    step_id: str,
    up: Stream[KafkaSourceMessage[K, MaybeStrBytes]],
    deserializer: confluent_kafka.serialization.Deserializer,
) -> KafkaOpOut[KafkaSourceMessage[K, dict], KafkaError[K, MaybeStrBytes]]:
    """Deserialize Kafka message values.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg deserializer: To use.

    :returns: Stream of deserialized messages and a stream of
        deserialization errors.

    """

    def shim_mapper(
        msg: KafkaSourceMessage[K, MaybeStrBytes],
    ) -> Union[KafkaSourceMessage[K, dict], KafkaError[K, MaybeStrBytes]]:
        try:
            value = deserializer(
                msg.value, ctx=SerializationContext(msg.topic, MessageField.VALUE)
            )
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
    up: Stream[KafkaSourceMessage[MaybeStrBytes, MaybeStrBytes]],
    *,
    key_deserializer: confluent_kafka.serialization.Deserializer,
    val_deserializer: confluent_kafka.serialization.Deserializer,
) -> KafkaOpOut[
    KafkaSourceMessage[dict, dict], KafkaError[MaybeStrBytes, MaybeStrBytes]
]:
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
        msg: KafkaSourceMessage[MaybeStrBytes, MaybeStrBytes],
    ) -> Union[
        KafkaSourceMessage[dict, dict], KafkaError[MaybeStrBytes, MaybeStrBytes]
    ]:
        try:
            key = key_deserializer(
                msg.key, ctx=SerializationContext(msg.topic, MessageField.KEY)
            )
        except Exception as e:
            err = ConfluentKafkaError(ConfluentKafkaError._KEY_DESERIALIZATION, f"{e}")
            return KafkaError(err, msg)

        try:
            value = val_deserializer(
                msg.value, ctx=SerializationContext(msg.topic, MessageField.VALUE)
            )
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
    up: Stream[Union[KafkaSourceMessage[dict, V], KafkaSinkMessage[dict, V]]],
    serializer: confluent_kafka.serialization.Serializer,
) -> Stream[KafkaSinkMessage[bytes, V]]:
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

    def shim_mapper(msg: KafkaSinkMessage[dict, V]) -> KafkaSinkMessage[bytes, V]:
        key = serializer(msg.key, ctx=SerializationContext(msg.topic, MessageField.KEY))
        return msg._with_key(key)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)


@operator
def serialize_value(
    step_id: str,
    up: Stream[Union[KafkaSourceMessage[K, dict], KafkaSinkMessage[K, dict]]],
    serializer: confluent_kafka.serialization.Serializer,
) -> Stream[KafkaSinkMessage[K, bytes]]:
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

    def shim_mapper(msg: KafkaSinkMessage[K, dict]) -> KafkaSinkMessage[K, bytes]:
        value = serializer(
            msg.value, ctx=SerializationContext(msg.topic, MessageField.VALUE)
        )
        return msg._with_value(value)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)


@operator
def serialize(
    step_id: str,
    up: Stream[Union[KafkaSourceMessage[dict, dict], KafkaSinkMessage[dict, dict]]],
    *,
    key_serializer: confluent_kafka.serialization.Serializer,
    val_serializer: confluent_kafka.serialization.Serializer,
) -> Stream[KafkaSinkMessage[bytes, bytes]]:
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

    def shim_mapper(
        msg: KafkaSinkMessage[dict, dict],
    ) -> KafkaSinkMessage[bytes, bytes]:
        key = key_serializer(
            msg.key, ctx=SerializationContext(msg.topic, MessageField.KEY)
        )
        value = val_serializer(
            msg.value, ctx=SerializationContext(msg.topic, MessageField.VALUE)
        )
        return msg._with_key_and_value(key, value)

    return _to_sink("to_sink", up).then(op.map, "map", shim_mapper)
