"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from .registry import (
    ConfluentSchemaRegistry,
    RedpandaSchemaRegistry,
    SchemaConf,
    SchemaRegistry,
)

__all__ = [
    "KafkaSource",
    "KafkaSink",
    "RedpandaSchemaRegistry",
    "ConfluentSchemaRegistry",
    "SchemaConf",
]

logger = logging.getLogger(__name__)


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
        key_deserializer,
        value_deserializer,
    ):
        self._offset = starting_offset if resume_state is None else resume_state
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._batch_size = batch_size
        self._eof = False
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

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
            if self._key_deserializer is not None:
                # This can fail.
                # If deserialization raises an exception, we bubble it up
                key = self._key_deserializer.de(msg.key(), msg.topic())
            if self._value_deserializer is not None:
                value = self._value_deserializer.de(msg.value(), msg.topic())

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
            key_deserializer = None
            value_deserializer = None
        else:
            key_deserializer = self._registry.key_deserializer()
            value_deserializer = self._registry.value_deserializer()

        return _KafkaSourcePartition(
            consumer,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            key_deserializer,
            value_deserializer,
        )


class _KafkaSinkPartition(StatelessSinkPartition):
    def __init__(self, producer, topic, key_serializer, value_serializer):
        self._producer = producer
        self._topic = topic
        self._key_serializer = key_serializer
        self._value_serializer = value_serializer

    def write_batch(self, batch):
        for _key, _value in batch:

            value = _value
            if self._value_serializer is not None:
                value = self._value_serializer.ser(value, self._topic)

            key = _key
            if self._key_serializer is not None:
                key = self._key_serializer.ser(key, self._topic)

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

        key_serializer = None
        value_serializer = None
        if self._registry is not None:
            key_serializer = self._registry.key_serializer()
            value_serializer = self._registry.value_serializer()

        return _KafkaSinkPartition(
            producer, self._topic, key_serializer, value_serializer
        )
