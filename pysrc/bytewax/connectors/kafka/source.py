"""KafkaSource."""

from datetime import datetime
from typing import Dict, Iterable, List, Optional, Union

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.admin import AdminClient

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from ._types import MaybeStrBytes
from .error import KafkaError
from .message import KafkaSourceMessage

# Some type aliases
_KafkaMessage = KafkaSourceMessage[MaybeStrBytes, MaybeStrBytes]
_KafkaError = KafkaError[MaybeStrBytes, MaybeStrBytes]
_KafkaItem = Union[_KafkaMessage, _KafkaError]


def _list_parts(client: AdminClient, topics: Iterable[str]) -> Iterable[str]:
    for topic in topics:
        # List topics one-by-one so if auto-create is turned on,
        # we respect that.
        cluster_metadata = client.list_topics(topic)
        assert cluster_metadata.topics is not None
        topic_metadata = cluster_metadata.topics[topic]
        if topic_metadata.error is not None:
            msg = (
                f"error listing partitions for Kafka topic `{topic!r}`: "
                f"{topic_metadata.error.str()}"
            )
            raise RuntimeError(msg)
        assert topic_metadata.partitions is not None
        part_idxs = topic_metadata.partitions.keys()
        for i in part_idxs:
            yield f"{i}-{topic}"


class _KafkaSourcePartition(StatefulSourcePartition[_KafkaItem, Optional[int]]):
    def __init__(
        self,
        consumer,
        topic,
        part_idx,
        starting_offset,
        resume_state,
        batch_size,
        raise_on_errors,
    ):
        self._offset = starting_offset if resume_state is None else resume_state
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._batch_size = batch_size
        self._eof = False
        self._raise_on_errors = raise_on_errors

    def next_batch(self, sched: Optional[datetime]) -> List[_KafkaItem]:
        if self._eof:
            raise StopIteration()

        msgs = self._consumer.consume(self._batch_size, 0.001)

        batch: List[_KafkaItem] = []
        last_offset = None
        for msg in msgs:
            error = None
            if msg.error() is not None:
                if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                    # Set self._eof to True and only raise StopIteration
                    # at the next cycle, so that we can emit messages in
                    # this batch
                    self._eof = True
                    break
                elif self._raise_on_errors:
                    # Discard all the messages in this batch too
                    err_msg = (
                        f"error consuming from Kafka topic `{self._topic!r}`: "
                        f"{msg.error()}"
                    )
                    raise RuntimeError(err_msg)
                else:
                    error = msg.error()

            kafka_msg = KafkaSourceMessage(
                key=msg.key(),
                value=msg.value(),
                topic=msg.topic(),
                headers=msg.headers(),
                latency=msg.latency(),
                offset=msg.offset(),
                partition=msg.partition(),
                timestamp=msg.timestamp(),
            )
            if error is None:
                batch.append(kafka_msg)
            else:
                batch.append(KafkaError(error, kafka_msg))
            last_offset = msg.offset()

        # Resume reading from the next message, not this one.
        if last_offset is not None:
            self._offset = last_offset + 1
        return batch

    def snapshot(self) -> Optional[int]:
        return self._offset

    def close(self) -> None:
        self._consumer.close()


class KafkaSource(FixedPartitionedSource[_KafkaItem, Optional[int]]):
    """Use a set of Kafka topics as an input source.

    Partitions are the unit of parallelism.
    Can support exactly-once processing.

    Messages are emitted into the dataflow
    as `bytewax.connectors.kafka.KafkaMessage` objects.
    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Optional[Dict[str, str]] = None,
        batch_size: int = 1000,
        raise_on_errors: bool = True,
    ):
        """Init.

        Args:
            brokers: List of `host:port` strings of Kafka brokers.

            topics: List of topics to consume from.

            tail: Whether to wait for new data on this topic when the
                end is initially reached.

            starting_offset: Can be either `confluent_kafka.OFFSET_BEGINNING` or
                `confluent_kafka.OFFSET_END`. Defaults to beginning of
                topic.

            add_config: Any additional configuration properties. See [the
                `rdkafka`
                documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
                for options.

            batch_size: How many messages to consume at most at each poll.
                This is 1000 by default. The default setting is a suitable
                starting point for higher throughput dataflows, but can be
                tuned lower to potentially decrease individual message
                processing latency.

            raise_on_errors: If set to False, errors won't stop the dataflow, and the
                KafkaMessage.error field will be set. It's up to you to
                properly handle the error later.
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
        self._raise_on_errors = raise_on_errors

    def list_parts(self) -> List[str]:
        """Each Kafka partition is an input partition."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        client = AdminClient(config)

        return list(_list_parts(client, self._topics))

    def build_part(
        self, now: datetime, for_part: str, resume_state: Optional[int]
    ) -> _KafkaSourcePartition:
        """See ABC docstring."""
        idx, topic = for_part.split("-", 1)
        part_idx = int(idx)
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
        return _KafkaSourcePartition(
            consumer,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            self._raise_on_errors,
        )
