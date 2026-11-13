"""Benchmarks of the Kafka connector's own per-message overhead.

The confluent-kafka clients are replaced with no-op fakes, so these
measure only bytewax's work around librdkafka (wrapping messages,
tracking offsets, building produce calls, running the dataflow) and
need no broker. They run with the other benchmarks (`just
test-benchmark`) and are skipped by `--benchmark-skip`.

"""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import bytewax.connectors.kafka as kafka_mod
import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
)
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource, run_main
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from pytest import fixture

TOPIC = "bench"
BATCH = 1_000
MESSAGES = 20_000
VALUE = b"x" * 100


class _FakeMsg:
    """Stand-in for `confluent_kafka.Message`."""

    __slots__ = ("_offset", "_error")

    def __init__(self, offset: int, error: Optional[ConfluentKafkaError] = None):
        self._offset = offset
        self._error = error

    def error(self):
        return self._error

    def headers(self):
        return None

    def key(self):
        return b"key"

    def value(self):
        return VALUE

    def topic(self):
        return TOPIC

    def latency(self):
        return None

    def offset(self):
        return self._offset

    def partition(self):
        return 0

    def timestamp(self):
        return (1, self._offset)


_EOF = ConfluentKafkaError(ConfluentKafkaError._PARTITION_EOF)


class _FakeConsumer:
    """Serves `MESSAGES` messages in batches of `BATCH`, then EOF."""

    batches: List[List[_FakeMsg]] = [
        [_FakeMsg(start + i) for i in range(BATCH)]
        for start in range(0, MESSAGES, BATCH)
    ]

    def __init__(self, config: Dict[str, Any]):
        self._next = 0

    def assign(self, parts):
        pass

    def consume(self, num_messages, timeout):
        if self._next < len(self.batches):
            batch = self.batches[self._next]
            self._next += 1
            return batch
        return [_FakeMsg(MESSAGES, _EOF)]

    def close(self):
        pass


class _FakeProducer:
    def __init__(self, config: Dict[str, Any]):
        pass

    def produce(self, **kwargs):
        pass

    def poll(self, timeout):
        pass

    def flush(self):
        pass


class _FakeAdminClient:
    """Lists a single partition for any topic."""

    def __init__(self, config: Dict[str, Any]):
        pass

    def poll(self, timeout):
        pass

    def list_topics(self, topic, timeout):
        partition = SimpleNamespace(id=0)
        meta = SimpleNamespace(partitions={0: partition}, error=None)
        return SimpleNamespace(topics={topic: meta})


@fixture
def fake_clients(monkeypatch):
    monkeypatch.setattr(kafka_mod, "AdminClient", _FakeAdminClient)
    monkeypatch.setattr(kafka_mod, "Consumer", _FakeConsumer)
    monkeypatch.setattr(kafka_mod, "Producer", _FakeProducer)


class _CountPartition(StatelessSinkPartition[Any]):
    def __init__(self, counts: List[int]):
        self._counts = counts

    def write_batch(self, items: List[Any]) -> None:
        self._counts[0] += len(items)


class _CountSink(DynamicSink[Any]):
    """Counts items instead of keeping them, to keep the sink cheap."""

    def __init__(self):
        self.counts = [0]

    def build(self, step_id, worker_index, worker_count):
        return _CountPartition(self.counts)


def test_kafka_source_next_batch_benchmark(benchmark, fake_clients):
    src = KafkaSource(["broker:9092"], [TOPIC], tail=False, batch_size=BATCH)

    def run():
        part = src.build_part("bench", f"0-{TOPIC}", None)
        count = 0
        try:
            while True:
                count += len(part.next_batch())
        except StopIteration:
            pass
        assert count == MESSAGES

    benchmark(run)


def test_kafka_sink_write_batch_benchmark(benchmark, fake_clients):
    part = KafkaSink(["broker:9092"], TOPIC).build("bench", 0, 1)
    batch = [KafkaSinkMessage(b"key", VALUE) for _ in range(BATCH)]

    def run():
        for _ in range(MESSAGES // BATCH):
            part.write_batch(batch)

    benchmark(run)


def test_kafka_source_dataflow_benchmark(benchmark, fake_clients):
    sink = _CountSink()
    flow = Dataflow("bench")
    s = op.input(
        "in", flow, KafkaSource(["broker:9092"], [TOPIC], tail=False, batch_size=BATCH)
    )
    s = op.map("value_len", s, lambda m: len(m.value))
    op.output("out", s, sink)

    def run():
        sink.counts[0] = 0
        run_main(flow)
        assert sink.counts[0] == MESSAGES

    benchmark(run)


def test_kafka_sink_dataflow_benchmark(benchmark, fake_clients):
    inp = [KafkaSinkMessage(b"key", VALUE) for _ in range(MESSAGES)]
    flow = Dataflow("bench")
    s = op.input("in", flow, TestingSource(inp, batch_size=BATCH))
    op.output("out", s, KafkaSink(["broker:9092"], TOPIC))

    benchmark(lambda: run_main(flow))


def test_kafka_serde_operators_benchmark(benchmark):
    inp = [
        KafkaSourceMessage(f"key-{i}".encode(), VALUE, topic=TOPIC, offset=i)
        for i in range(MESSAGES)
    ]
    sink = _CountSink()
    flow = Dataflow("bench")
    s = op.input("in", flow, TestingSource(inp, batch_size=BATCH))
    decoded = kop.deserialize(
        "decode",
        s,
        key_deserializer=StringDeserializer(),
        val_deserializer=StringDeserializer(),
    )
    encoded = kop.serialize(
        "encode",
        decoded.oks,
        key_serializer=StringSerializer(),
        val_serializer=StringSerializer(),
    )
    op.output("out", encoded, sink)

    def run():
        sink.counts[0] = 0
        run_main(flow)
        assert sink.counts[0] == MESSAGES

    benchmark(run)
