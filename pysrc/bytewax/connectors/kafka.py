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

from bytewax.inputs import PartInput
from bytewax.outputs import DynamicOutput


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


class KafkaInput(PartInput):
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

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Dict[str, str] = None,
    ):
        add_config = add_config or {}

        if isinstance(brokers, str):
            raise TypeError("brokers must be an iterable and not a string")
        self.brokers = brokers
        if isinstance(topics, str):
            raise TypeError("topics must be an iterable and not a string")
        self.topics = topics
        self.tail = tail
        self.starting_offset = starting_offset
        self.add_config = add_config

    def list_parts(self):
        config = {
            "bootstrap.servers": ",".join(self.brokers),
        }
        config.update(self.add_config)
        client = AdminClient(config)

        return set(_list_parts(client, self.topics))

    def build_part(self, for_part, resume_state):
        part_idx, topic = for_part.split("-", 1)
        part_idx = int(part_idx)
        # TODO: Warn and then return None. This might be an indication
        # of dataflow continuation with a new topic (to enable
        # re-partitioning), which is fine.
        assert topic in self.topics, "Can't resume from different set of Kafka topics"

        resume_offset = resume_state or self.starting_offset

        config = {
            # We'll manage our own "consumer group" via recovery
            # system.
            "group.id": "BYTEWAX_IGNORED",
            "enable.auto.commit": "false",
            "bootstrap.servers": ",".join(self.brokers),
            "enable.partition.eof": str(not self.tail),
        }
        config.update(self.add_config)
        consumer = Consumer(config)
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, resume_offset)])

        while True:
            msg = consumer.poll(0.001)  # seconds
            if msg is None:
                yield
            elif msg.error() is not None:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    consumer.close()
                    return
                else:
                    raise RuntimeError(
                        f"error consuming from Kafka topic `{topic!r}`: {msg.error()}"
                    )
            else:
                item = (msg.key(), msg.value())
                yield msg.offset(), item


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

        topic: Topic to consume from.

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

        self.brokers = brokers
        self.topic = topic
        self.add_config = add_config

    def build(self):
        config = {
            "bootstrap.servers": ",".join(self.brokers),
        }
        config.update(self.add_config)
        producer = Producer(config)

        def write(key_value):
            key, value = key_value
            producer.produce(self.topic, value, key)
            producer.flush()
            return None

        return write
