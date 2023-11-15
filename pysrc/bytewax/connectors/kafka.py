"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

"""
import io
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import avro.schema
import requests
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
)

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

__all__ = [
    "KafkaSource",
    "KafkaSink",
    "RedpandaSchemaRegistry",
    "ConfluentSchemaRegistry",
]

logger = logging.getLogger(__name__)


class SchemaSerializer(ABC):
    should_serialize_key: bool
    should_serialize_value: bool

    @abstractmethod
    def ser(self, obj: dict, *args, **kwargs) -> bytes:
        ...


class SchemaDeserializer(ABC):
    should_deserialize_key: bool
    should_deserialize_value: bool

    @abstractmethod
    def de(self, data: bytes, *args, **kwargs) -> dict:
        ...


class SchemaRegistry(ABC):
    @abstractmethod
    def serializer(self, *args, **kwargs) -> SchemaSerializer:
        ...

    @abstractmethod
    def deserializer(self, *args, **kwargs) -> SchemaDeserializer:
        ...


@dataclass(frozen=True)
class SchemaConf:
    schema_id: Optional[int]
    subject: Optional[str]
    version: Optional[int]

    def __post_init__(self):
        """Validate the data."""
        if self.schema_id is None:
            if self.subject is None:
                msg = "subject MUST be specified if no schema_id is provided"
                raise ValueError(msg)
        else:
            if self.subject is not None:
                logger.warning(
                    "both schema_id and subject specified, subject will be ignored"
                )


class _ConfluentAvroSerializer(SchemaSerializer):
    def __init__(self, client, key_schema_str, value_schema_str):
        self.key_serializer = AvroSerializer(client, key_schema_str)
        self.value_serializer = AvroSerializer(client, value_schema_str)

    def ser(self, data, topic, is_key):
        if is_key:
            if self.key_serializer is None:
                msg = "Missing key_schema in confluent serializer"
                raise ValueError(msg)
            ctx = SerializationContext(topic, MessageField.KEY)
            serializer = self.key_serializer
        else:
            if self.value_serializer is None:
                msg = "Missing value_schema in confluent serializer"
                raise ValueError(msg)
            ctx = SerializationContext(topic, MessageField.VALUE)
            serializer = self.value_serializer

        try:
            return serializer(data, ctx)
        except SerializationError as e:
            logger.error(f"Error serializing data: {data}")
            raise e


class _ConfluentAvroDeserializer(SchemaDeserializer):
    def __init__(self, client):
        self.deserializer = AvroDeserializer(client)

    def de(self, data, topic, is_key):
        field = MessageField.KEY if is_key else MessageField.VALUE
        ctx = SerializationContext(topic, field)
        try:
            return self.deserializer(data, ctx)
        except SerializationError as e:
            logger.error(f"Error deserializing data: {data}")
            raise e


class ConfluentSchemaRegistry(SchemaRegistry):
    """TODO."""

    def __init__(
        self,
        sr_conf: Dict,
        value_conf: Optional[SchemaConf],
        key_conf: Optional[SchemaConf],
    ):
        """Confluent's schema registry configuration for KafkaInput.

        Use either `schema_id` or `subject`+`version` to fetch the schema
        for the serializer from the registry.

        This client follows confluent_kafka's behavior, so the deserializer
        automatically fetches the correct schema for each message, even if
        different from the one specified here.

        Args:
            sr_conf:
                Configuration for `confluent_kafka.schema_registry.SchemaRegistryClient`
        """
        self._sr_conf = sr_conf
        self._value_conf = value_conf
        self._key_conf = key_conf

    @staticmethod
    def _get_schema_str(client, schema_conf) -> str:
        # Schema can bew retrieved by `schema_id`, or by
        # specifying a `subject` and a `version`, which
        # defaults to `latest`.
        if schema_conf.schema_id is not None:
            schema = client.get_schema(schema_conf.schema_id)
        else:
            # If schema_conf.version is None, `get_version`
            # defaults to `latest` here
            schema = client.get_version(schema_conf.subject, schema_conf.version).schema
        return schema.schema_str

    def serializer(self):
        """TODO."""
        if self.value_conf is None and self.key_conf is None:
            msg = (
                "At least one of value_conf and key_conf "
                "must be provided for serialization"
            )
            raise ValueError(msg)
        client = SchemaRegistryClient(self._sr_conf)
        value_schema_str = self._get_schema_str(client, self._value_conf)
        key_schema_str = self._get_schema_str(client, self._key_conf)
        return _ConfluentAvroSerializer(client, key_schema_str, value_schema_str)

    def deserializer(self):
        """TODO."""
        client = SchemaRegistryClient(self._sr_conf)
        return _ConfluentAvroDeserializer(client)


class RedpandaSchemaRegistry(SchemaRegistry):
    """TODO."""

    def __init__(
        self,
        base_url: str = "http://localhost:18081",
        input_value_conf: Optional[SchemaConf] = None,
        input_key_conf: Optional[SchemaConf] = None,
        output_value_conf: Optional[SchemaConf] = None,
        output_key_conf: Optional[SchemaConf] = None,
    ):
        """TODO."""
        self._base_url = base_url
        self._input_key_schema = self._get_schema_str(input_key_conf)
        self._input_value_schema = self._get_schema_str(input_value_conf)
        self._output_key_schema = self._get_schema_str(output_key_conf)
        self._output_value_schema = self._get_schema_str(output_value_conf)

    def _get_schema_str(self, schema_conf) -> Optional[str]:
        if schema_conf is None:
            return None
        if schema_conf.schema_id is not None:
            url = f"{self._base_url}/schemas/{schema_conf.schema_id}/schema"
        elif schema_conf.version is not None:
            url = (
                f"{self._base_url}/subjects/"
                f"{schema_conf.subject}/versions/"
                f"{schema_conf.version}/schema"
            )
        schema_content = requests.get(url)
        return avro.schema.parse(schema_content)

    def serializer(self, *args, **kwargs):
        """TODO."""
        return _AvroSerializer(self._output_key_schema, self._output_value_schema)

    def deserializer(self, *args, **kwargs):
        """TODO."""
        return _AvroDeserializer(self._input_key_schema, self._input_value_schema)


class _AvroSerializer(SchemaSerializer):
    def __init__(self, key_schema, value_schema):
        self._key_schema = key_schema
        self._value_schema = value_schema

    def serialize(self, data, topic, is_key):
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)

        if is_key:
            if self._key_schema is None:
                msg = "Missing key_schema in avro serializer"
                raise ValueError(msg)
            writer = DatumWriter(self._key_schema)
        else:
            if self._value_schema is None:
                msg = "Missing value_schema in avro serializer"
                raise ValueError(msg)
            writer = DatumWriter(self._value_schema)

        writer.write(data, encoder)
        return bytes_writer.getvalue()


class _AvroDeserializer(SchemaDeserializer):
    def __init__(self, key_schema, value_schema):
        self._key_reader = None
        self._value_reader = None
        if key_schema is not None:
            self._key_reader = DatumReader(key_schema)
        if value_schema is not None:
            self._value_reader = DatumReader(value_schema)

    def de(self, msg, _topic, is_key):
        message_bytes = io.BytesIO(msg)
        decoder = BinaryDecoder(message_bytes)

        if is_key:
            if self._key_reader is None:
                msg = "Missing key_schema in avro deserializer"
                raise ValueError(msg)
            return self._key_reader.read(decoder)
        else:
            if self._value_schema is None:
                msg = "Missing value_schema in avro deserializer"
                raise ValueError(msg)
            return self._value_reader.read(decoder)


def _list_parts(client, topics):
    for topic in topics:
        # List topics one-by-one so if auto-create is turned on,
        # we respect that.
        cluster_metadata = client.list_topics(topic)
        topic_metadata = cluster_metadata.topics[topic]
        if topic_metadata.error is not None:
            msg = (
                f"error listing partitions for Kafka topic `{topic!r}`: "
                f"{topic_metadata.error.str()}"
            )
            raise RuntimeError(msg)
        part_idxs = topic_metadata.partitions.keys()
        for i in part_idxs:
            yield f"{i}-{topic}"


@dataclass(frozen=True)
class KafkaMessage:
    """Class that holds a message from kafka with metadata."""

    key: Any
    value: Any

    topic: Any
    headers: Any
    latency: Any
    offset: Any
    partition: Any
    timestamp: Any


class _KafkaSourcePartition(StatefulSourcePartition):
    def __init__(
        self,
        consumer,
        topic,
        part_idx,
        starting_offset,
        resume_state,
        batch_size,
        deserializer,
    ):
        self._offset = starting_offset if resume_state is None else resume_state
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._batch_size = batch_size
        self._eof = False
        self._deserializer = deserializer

    def next_batch(self, _sched: datetime) -> List[Any]:
        if self._eof:
            raise StopIteration()

        msgs = self._consumer.consume(self._batch_size, 0.001)

        batch = []
        last_offset = None
        for msg in msgs:
            if msg.error() is not None:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Set self._eof to True and only raise StopIteration
                    # at the next cycle, so that we can emit messages in
                    # this batch
                    self._eof = True
                    break
                else:
                    # Discard all the messages in this batch too
                    err_msg = (
                        f"error consuming from Kafka topic `{self._topic!r}`: "
                        f"{msg.error()}"
                    )
                    raise RuntimeError(err_msg)
            # If no serializer is set, just use the raw value
            value = msg.value()
            key = msg.key()
            if self._deserializer is not None:
                # This can fail.
                # If deserialization raises an exception, we bubble it up
                key = self._deserializer.de(msg.key(), msg.topic(), True)
                value = self._deserializer.de(msg.value(), msg.topic(), False)

            k_msg = KafkaMessage(
                key=key,
                value=value,
                topic=msg.topic(),
                headers=msg.headers(),
                latency=msg.latency(),
                offset=msg.offset(),
                partition=msg.partition(),
                timestamp=msg.timestamp(),
            )
            batch.append((key, k_msg))
            last_offset = msg.offset()

        # Resume reading from the next message, not this one.
        if last_offset is not None:
            self._offset = last_offset + 1
        return batch

    def snapshot(self):
        return self._offset

    def close(self):
        self._consumer.close()


class KafkaSource(FixedPartitionedSource):
    """Use a set of Kafka topics as an input source.

    Kafka messages are emitted into the dataflow as two-tuples of
    `(key_bytes, value_bytes)`.

    Partitions are the unit of parallelism.

    Can support exactly-once processing.
    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Optional[Dict[str, str]] = None,
        batch_size: int = 1,
        schema_registry: Optional[SchemaRegistry] = None,
    ):
        """Init.

        Args:
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
                Any additional configuration properties. See [the
                `rdkafka`
                documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
                for options.
            batch_size:
                How many messages to consume at most at each poll.
                This is 1 by default, which means messages will be
                consumed one at a time. The default setting is suited
                for lower latency, but negatively affects
                throughput. If you need higher throughput, set this to
                a higher value (eg: 1000)
            schema_registry:
                A schema registry to use to retrieve a schema to decode
                messages. See:
                - `bytewax.connectors.kafka.RedpandaSchemaRegistry`
                - `bytewax.connectors.kafka.ConfluentSchemaRegistry`

        """
        if isinstance(brokers, str):
            msg = "brokers must be an iterable and not a string"
            raise TypeError(msg)
        self._brokers = brokers
        if isinstance(topics, str):
            msg = "topics must be an iterable and not a string"
            raise TypeError(msg)
        self._topics = topics
        self._tail = tail
        self._starting_offset = starting_offset
        self._add_config = {} if add_config is None else add_config
        self._batch_size = batch_size
        self._registry = schema_registry

    def list_parts(self):
        """Each Kafka partition is an input partition."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        client = AdminClient(config)

        return list(_list_parts(client, self._topics))

    def build_part(self, _now: datetime, for_part: str, resume_state: Optional[Any]):
        """See ABC docstring."""
        part_idx, topic = for_part.split("-", 1)
        part_idx = int(part_idx)
        # TODO: Warn and then return None. This might be an indication
        # of dataflow continuation with a new topic (to enable
        # re-partitioning), which is fine.
        assert topic in self._topics, "Can't resume from different set of Kafka topics"

        config = {
            # We'll manage our own "consumer group" via recovery
            # system.
            "group.id": "BYTEWAX_IGNORED",
            "enable.auto.commit": "false",
            "bootstrap.servers": ",".join(self._brokers),
            "enable.partition.eof": str(not self._tail),
        }
        config.update(self._add_config)
        consumer = Consumer(config)
        if self._registry is None:
            deserializer = None
        else:
            deserializer = self._registry.deserializer(topic)

        return _KafkaSourcePartition(
            consumer,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            deserializer,
        )


class _KafkaSinkPartition(StatelessSinkPartition):
    def __init__(self, producer, topic, serializer):
        self._producer = producer
        self._topic = topic
        self._serializer = serializer

    def write_batch(self, batch):
        for key, msg in batch:
            if isinstance(msg, KafkaMessage):
                value = msg.value
            else:
                value = msg

            if self._serializer is not None:
                value = self._serializer.serialize(value)
            self._producer.produce(self._topic, value, key)
            self._producer.poll(0)
        self._producer.flush()

    def close(self):
        self._producer.flush()


class KafkaSink(DynamicSink):
    """Use a single Kafka topic as an output sink.

    Items consumed from the dataflow must look like two-tuples of
    `(key_bytes, value_bytes)`. Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topic: str,
        add_config: Optional[Dict[str, str]] = None,
        schema_registry: Optional[SchemaRegistry] = None,
    ):
        """Init.

        Args:
            brokers:
                List of `host:port` strings of Kafka brokers.
            topic:
                Topic to produce to.
            add_config:
                Any additional configuration properties. See [the
                `rdkafka`
                documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
                for options.
            schema_registry:
                A schema registry to use to retrieve a schema to encode
                messages. See:
                - `bytewax.connectors.kafka.RedpandaSchemaRegistry`
                - `bytewax.connectors.kafka.ConfluentSchemaRegistry`

        """
        self._brokers = brokers
        self._topic = topic
        self._add_config = {} if add_config is None else add_config
        self._registry = schema_registry

    def build(self, worker_index, worker_count):
        """See ABC docstring."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        producer = Producer(config)
        serializer = None
        if self._registry is not None:
            serializer = self._registry.serializer(self._topic)
        return _KafkaSinkPartition(producer, self._topic, serializer)


# Why not?
# @operator()
# def kafka_input(up: Dataflow, broker, topic, add_config, schema_registry)
#     ...
#
# registry = ConfluentSchemaRegistry(
#     sr_conf,
#     value_conf=SchemaConf(subject="out_schema", version=1),
#     key_conf=SchemaConf(subject="out_key_schema"),
# )
# # OR
# registry = RedpandaSchemaRegistry(
#     input_value_conf=SchemaConf(schema_id=12)
#     input_key_conf=SchemaConf(subject="input_schema")
#     output_value_conf=SchemaConf(schema_id=13)
#     output_key_conf=SchemaConf(subject="output_schema", version=2)
# )

# flow = Dataflow("from kafka to kafka")
# inp = flow.kafkain("in", topics=["test"], registry)
# inp.kafkaout("out", topic="out_test", registry)
