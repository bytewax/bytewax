"""KafkaSink."""

from typing import Dict, Iterable, List, Optional, Tuple

from confluent_kafka import Producer

from bytewax.outputs import DynamicSink, StatelessSinkPartition

from ._types import MaybeStrBytes


class _KafkaSinkPartition(StatelessSinkPartition[Tuple[MaybeStrBytes, MaybeStrBytes]]):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    def write_batch(self, items: List[Tuple[MaybeStrBytes, MaybeStrBytes]]) -> None:
        for key, value in items:
            self._producer.produce(self._topic, value, key)
            self._producer.poll(0)
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()


class KafkaSink(DynamicSink[Tuple[MaybeStrBytes, MaybeStrBytes]]):
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
