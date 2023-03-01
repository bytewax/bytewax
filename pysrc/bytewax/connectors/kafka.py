"""Connectors for [Kafka](https://kafka.apache.org).

"""
from typing import Dict, Iterable

from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient

from bytewax.inputs import PartInput


def _stateful_read_part(consumer):
    while True:
        msg = consumer.poll(0.001)  # seconds
        if msg is None:
            yield
        elif msg.error() is not None:
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                raise RuntimeError(msg.error())
        else:
            item = (msg.key(), msg.value())
            yield msg.offset(), item


class KafkaInput(PartInput):
    """Use [Kafka](https://kafka.apache.org) topics as an input
    source.

    Kafka messages are emitted into the dataflow as two-tuples of
    `(key_bytes, payload_bytes)`.

    Partitions are the unit of parallelism.

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

        self.brokers = brokers
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
        cluster_metadata = client.list_topics()

        for found_topic in self.topics:
            part_idxs = cluster_metadata.topics[found_topic].partitions.keys()
            for i in part_idxs:
                yield f"{i}-{found_topic}"

    def build_part(self, for_part, resume_state):
        found_part_idx, found_topic = for_part.split("-", 1)
        found_part_idx = int(found_part_idx)
        # TODO: Warn and then return None. This might be an indication
        # of dataflow continuation with a new topic (to enable
        # re-partitioning), which is fine.
        assert (
            found_topic in self.topics
        ), "Can't resume from different set of Kafka topics"

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
        consumer.assign([TopicPartition(found_topic, found_part_idx, resume_offset)])

        return _stateful_read_part(consumer)
