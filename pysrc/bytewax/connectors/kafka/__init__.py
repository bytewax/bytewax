"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

The input source returns a stream of
{py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage`. See the
docstring for its use.

You can use {py:obj}`~bytewax.connectors.kafka.KafkaSource` and
{py:obj}`~bytewax.connectors.kafka.KafkaSink` directly:

```python
>>> from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
>>> from bytewax import operators as op
>>> from bytewax.dataflow import Dataflow
>>>
>>> brokers = ["localhost:19092"]
>>> flow = Dataflow("example")
>>> kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["in-topic"]))
>>> processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
>>> op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))
```

Or the custom operators:

```python
>>> from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
>>> from bytewax import operators as op
>>> from bytewax.dataflow import Dataflow
>>>
>>> brokers = ["localhost:19092"]
>>> flow = Dataflow("example")
>>> kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
>>> errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")
>>> processed = op.map("map", kinp.oks, lambda x: KafkaSinkMessage(x.key, x.value))
>>> kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")
```

"""
import json
from dataclasses import dataclass, field
from typing import Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge
from typing_extensions import TypeAlias

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

K = TypeVar("K")
"""Type of key in Kafka message."""

V = TypeVar("V")
"""Type of value in a Kafka message."""

K_co = TypeVar("K_co", covariant=True)
"""Type of key in Kafka message."""

V_co = TypeVar("V_co", covariant=True)
"""Type of value in a Kafka message."""

K2 = TypeVar("K2")
"""Type of key in a modified Kafka message."""

V2 = TypeVar("V2")
"""Type of value in a modified Kafka message."""

MaybeStrBytes: TypeAlias = Union[str, bytes, None]
"""Kafka message keys and values are optional."""


@dataclass(frozen=True)
class KafkaSourceMessage(Generic[K, V]):
    """Message read from Kafka."""

    key: K
    value: V

    topic: Optional[str] = field(default=None)
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    latency: Optional[float] = field(default=None)
    offset: Optional[int] = field(default=None)
    partition: Optional[int] = field(default=None)
    timestamp: Optional[Tuple[int, int]] = field(default=None)

    def to_sink(self) -> "KafkaSinkMessage[K, V]":
        """Convert a source message to be used with a sink.

        Only {py:obj}`key`, {py:obj}`value` and {py:obj}`timestamp`
        are used.

        """
        return KafkaSinkMessage(key=self.key, value=self.value, headers=self.headers)

    def _with_key(self, key: K2) -> "KafkaSourceMessage[K2, V]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSourceMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSourceMessage[K, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSourceMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSourceMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSourceMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )


SerializedKafkaSourceMessage: TypeAlias = KafkaSourceMessage[
    MaybeStrBytes, MaybeStrBytes
]
"""A fully serialized Kafka message from the consumer."""


@dataclass(frozen=True)
class KafkaError(Generic[K, V]):
    """Error from a {py:obj}`KafkaSource`."""

    err: ConfluentKafkaError
    """Underlying error from the consumer."""

    msg: KafkaSourceMessage[K, V]
    """Message attached to that error."""


KafkaSourceError: TypeAlias = KafkaError[MaybeStrBytes, MaybeStrBytes]
"""An error from the Kafka source with original message."""

SerializedKafkaSourceResult: TypeAlias = Union[
    SerializedKafkaSourceMessage, KafkaSourceError
]
"""Items emitted from the Kafka source.

Might be either raw serialized messages or an error from the consumer.

"""


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


class _KafkaSourcePartition(
    StatefulSourcePartition[SerializedKafkaSourceResult, Optional[int]]
):
    def __init__(
        self,
        step_id: str,
        config: dict,
        topic: str,
        part_idx: int,
        starting_offset: int,
        resume_state: Optional[int],
        batch_size: int,
        raise_on_errors: bool,
    ):
        self._offset = starting_offset if resume_state is None else resume_state
        print(f"starting offset: {starting_offset}")
        # Collect metrics from Kafka every 1s
        config.update({"stats_cb": self._process_stats})
        consumer = Consumer(config)
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._part_idx = part_idx
        self._batch_size = batch_size
        self._eof = False
        self._raise_on_errors = raise_on_errors

        # Set up metrics for Kafka
        self._consumer_lag = Gauge(
            "bytewax_kafka_consumer_lag",
            "Difference between last offset on the broker "
            "and the currently consumed offset.",
            ["step_id", "topic", "partition"],
        )
        # Labels to use when recording metrics
        self._metrics_labels = {
            "step_id": step_id,
            "topic": self._topic,
            "partition": str(self._part_idx),
        }

    def _process_stats(self, json_stats: str):
        """Process stats collected by librdkafka.

        This function is called by librdkafka based on the
        `statistics.interval.ms` config setting.

        For more information about the `json_stats` payload, see
        https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
        """
        partition_stats = json.loads(json_stats)["topics"][self._topic]["partitions"][
            str(self._part_idx)
        ]
        # The lag value here would be calculated incorrectly when using values
        # like OFFSET_STORED, or OFFSET_BEGINNING
        if self._offset > 0:
            self._consumer_lag.labels(**self._metrics_labels).set(
                partition_stats["ls_offset"] - self._offset
            )

    def next_batch(self) -> List[SerializedKafkaSourceResult]:
        if self._eof:
            raise StopIteration()

        msgs = self._consumer.consume(self._batch_size, 0.001)

        batch: List[SerializedKafkaSourceResult] = []
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


class KafkaSource(FixedPartitionedSource[SerializedKafkaSourceResult, Optional[int]]):
    """Use a set of Kafka topics as an input source.

    Partitions are the unit of parallelism.
    Can support exactly-once processing.

    Messages are emitted into the dataflow as
    {py:obj}`SerializedKafkaSourceResult` objects.

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

        :arg brokers: List of `host:port` strings of Kafka brokers.

        :arg topics: List of topics to consume from.

        :arg tail: Whether to wait for new data on this topic when the end
            is initially reached.

        :arg starting_offset: Can be either
            `confluent_kafka.OFFSET_BEGINNING` or
            `confluent_kafka.OFFSET_END`. Defaults to beginning of
            topic.

        :arg add_config: Any additional configuration properties. See
            [the `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

        :arg batch_size: How many messages to consume at most at each
            poll. This is 1000 by default. The default setting is a
            suitable starting point for higher throughput dataflows,
            but can be tuned lower to potentially decrease individual
            message processing latency.

        :arg raise_on_errors: If set to False, errors won't stop the
            dataflow, and will be emitted into the dataflow.

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
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _KafkaSourcePartition:
        """See ABC docstring."""
        idx, topic = for_part.split("-", 1)
        part_idx = int(idx)
        # TODO: Warn and then return None. This might be an indication
        # of dataflow continuation with a new topic (to enable
        # re-partitioning), which is fine.
        assert topic in self._topics, "Can't resume from different set of Kafka topics"

        config = {
            # We'll manage our own "consumer group" via the recovery
            # system.
            "group.id": "BYTEWAX_IGNORED",
            "enable.auto.commit": "false",
            "bootstrap.servers": ",".join(self._brokers),
            "enable.partition.eof": str(not self._tail),
            "statistics.interval.ms": 1000,
        }
        config.update(self._add_config)
        return _KafkaSourcePartition(
            step_id,
            config,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            self._raise_on_errors,
        )


@dataclass(frozen=True)
class KafkaSinkMessage(Generic[K_co, V_co]):
    """Message to be written to Kafka."""

    key: K_co
    value: V_co

    topic: Optional[str] = None
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    partition: Optional[int] = None
    timestamp: int = 0

    def _with_key(self, key: K2) -> "KafkaSinkMessage[K2, V_co]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSinkMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSinkMessage[K_co, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSinkMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSinkMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSinkMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )


SerializedKafkaSinkMessage: TypeAlias = KafkaSinkMessage[MaybeStrBytes, MaybeStrBytes]
"""A fully serialized Kafka message ready for the producer.

Both key and value are optional.

"""


class _KafkaSinkPartition(StatelessSinkPartition[SerializedKafkaSinkMessage]):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    def write_batch(self, items: List[SerializedKafkaSinkMessage]) -> None:
        for msg in items:
            topic = self._topic if msg.topic is None else msg.topic
            if topic is None:
                err = f"No topic to produce to for {msg}"
                raise RuntimeError(err)

            self._producer.produce(
                value=msg.value,
                key=msg.key,
                headers=msg.headers,
                topic=topic,
                timestamp=msg.timestamp,
            )
            self._producer.poll(0)
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()


class KafkaSink(DynamicSink[SerializedKafkaSinkMessage]):
    """Use a single Kafka topic as an output sink.

    Items consumed from the dataflow must be
    {py:obj}`SerializedKafkaSinkMessage`.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        # Optional with no defaults, so you have to explicitely pass
        # `topic=None` if you want to use the topic from the messages
        topic: Optional[str],
        add_config: Optional[Dict[str, str]] = None,
    ):
        """Init.

        :arg brokers: List of `host:port` strings of Kafka brokers.

        :arg topic: Topic to produce to. If it's `None`, the topic to
            produce to will be read in each
            {py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage`.

        :arg add_config: Any additional configuration properties. See
            [the `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

        """
        self._brokers = brokers
        self._topic = topic
        self._add_config = {} if add_config is None else add_config

    def build(self, worker_index: int, worker_count: int) -> _KafkaSinkPartition:
        """See ABC docstring."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        producer = Producer(config)

        return _KafkaSinkPartition(producer, self._topic)
