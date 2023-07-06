"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

"""
from typing import Dict, Iterable

from confluent_kafka import (
    Consumer,
    KafkaError,
    OFFSET_BEGINNING,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient

from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.outputs import DynamicOutput, StatelessSink

__all__ = [
    "KafkaInput",
    "KafkaOutput",
]


def _list_parts(client, topics):
    for topic in topics:
        # List topics one-by-one so if auto-create is turned on,
        # we respect that.
        cluster_metadata = client.list_topics(topic)
        topic_metadata = cluster_metadata.topics[topic]
        if topic_metadata.error is not None:
            raise RuntimeError(
                f"error listing partitions for Kafka topic `{topic!r}`: "
                f"{topic_metadata.error.str()}"
            )
        part_idxs = topic_metadata.partitions.keys()
        for i in part_idxs:
            yield f"{i}-{topic}"


class _KafkaSource(StatefulSource):
    def __init__(
        self,
        consumer,
        topic,
        part_idx,
        starting_offset,
        resume_state,
        batch_size,
    ):
        self._offset = resume_state or starting_offset
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._batch_size = batch_size
        self._eof = False

    def next(self):
        if self._eof:
            raise StopIteration()

        msgs = self._consumer.consume(self._batch_size, 0.001)

        result = []
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
                    raise RuntimeError(
                        f"error consuming from Kafka topic `{self.topic!r}`: {msg.error()}"
                    )
            result.append((msg.key(), msg.value()))
            # Resume reading from the next message, not this one.
            self._offset = msg.offset() + 1

        return result

    def snapshot(self):
        return self._offset

    def close(self):
        self._consumer.close()


class KafkaInput(PartitionedInput):
    """Use [Kafka](https://kafka.apache.org) topics as an input
    source.

    Kafka messages are emitted into the dataflow as two-tuples of
    `(key_bytes, value_bytes)`.

    Partitions are the unit of parallelism.

    Can support exactly-once processing.

    Args:

        brokers: List of `host:port` strings of Kafka brokers.

        topics: List of topics to consume from.

        tail: Whether to wait for new data on this topic when the end
            is initially reached.

        starting_offset: Can be either
            `confluent_kafka.OFFSET_BEGINNING` or
            `confluent_kafka.OFFSET_END`. Defaults to beginning of
            topic.

        add_config: Any additional configuration properties. See [the
            `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

        batch_size: How many messages to consume at most at each poll.
            This is 1 by default, which means messages will be consumed
            one at a time. The default setting is suited for lower latency,
            but negatively affects throughput. If you need higher
            throughput, set this to a higher value (eg: 1000)
    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Dict[str, str] = None,
        batch_size: int = 1,
    ):
        add_config = add_config or {}

        if isinstance(brokers, str):
            raise TypeError("brokers must be an iterable and not a string")
        self._brokers = brokers
        if isinstance(topics, str):
            raise TypeError("topics must be an iterable and not a string")
        self._topics = topics
        self._tail = tail
        self._starting_offset = starting_offset
        self._add_config = add_config
        self._batch_size = batch_size

    def list_parts(self):
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        client = AdminClient(config)

        return set(_list_parts(client, self._topics))

    def build_part(self, for_part, resume_state):
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
        return _KafkaSource(
            consumer,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
        )


class _KafkaSink(StatelessSink):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    def write(self, key_value):
        key, value = key_value
        self._producer.produce(self._topic, value, key)
        self._producer.flush()

    def close(self):
        self._producer.flush()


class KafkaOutput(DynamicOutput):
    """Use a single [Kafka](https://kafka.apache.org) topic as an
    output sink.

    Items consumed from the dataflow must look like two-tuples of
    `(key_bytes, value_bytes)`. Default partition routing is used.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    Args:

        brokers: List of `host:port` strings of Kafka brokers.

        topic: Topic to produce to.

        add_config: Any additional configuration properties. See [the
            `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topic: str,
        add_config: Dict[str, str] = None,
    ):
        add_config = add_config or {}

        self._brokers = brokers
        self._topic = topic
        self._add_config = add_config

    def build(self, worker_index, worker_count):
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        producer = Producer(config)
        return _KafkaSink(producer, self._topic)
