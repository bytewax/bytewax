"""Integration tests for bytewax wheel functionality.

Tests wheel installation, operator coverage, Kafka end-to-end,
windowed aggregation, recovery, multi-worker, and error scenarios.
"""

import importlib
import json
import os
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax._metrics import generate_python_metrics
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.operators import StatefulBatchLogic
from bytewax.operators.windowing import (
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
    collect_window,
    fold_window,
)
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import fixture, mark, raises

try:
    from bytewax.connectors.kafka import (
        KafkaSink,
        KafkaSinkMessage,
        KafkaSource,
    )
    from bytewax.connectors.kafka import operators as kop
    from confluent_kafka import (
        OFFSET_BEGINNING,
        Consumer,
        KafkaError,
        Producer,
        TopicPartition,
    )
    from confluent_kafka.admin import AdminClient, NewTopic

    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False


ZERO_TD = timedelta(seconds=0)
FIVE_TD = timedelta(seconds=5)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")

needs_kafka = mark.skipif(
    "TEST_KAFKA_BROKER" not in os.environ or not HAS_KAFKA,
    reason="Set `TEST_KAFKA_BROKER` env var and install kafka extras to run",
)


# ---------------------------------------------------------------------------
# Kafka helpers & fixtures
# ---------------------------------------------------------------------------


def _kafka_config():
    cluster_api_key = os.environ.get("CLUSTER_API_KEY")
    cluster_api_secret = os.environ.get("CLUSTER_API_SECRET")
    if cluster_api_key and cluster_api_secret:
        return {
            "bootstrap.servers": KAFKA_BROKER,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": cluster_api_key,
            "sasl.password": cluster_api_secret,
        }
    return {"bootstrap.servers": KAFKA_BROKER}


@fixture
def kafka_config():
    return _kafka_config()


@fixture
def tmp_topic(request):
    config = _kafka_config()
    client = AdminClient(config)
    topic_name = f"integ_{request.node.name}_{uuid.uuid4()}"
    wait_futures = client.create_topics(
        [NewTopic(topic_name, 3)], operation_timeout=5.0
    )
    for fut in wait_futures.values():
        fut.result()
    yield topic_name
    del_futures = client.delete_topics([topic_name], operation_timeout=5.0)
    for fut in del_futures.values():
        try:
            fut.result()
        except Exception:
            pass


tmp_topic_in = tmp_topic
tmp_topic_out = tmp_topic


@fixture
def producer(kafka_config):
    return Producer(kafka_config)


def consume_all(config, topic, timeout=10.0):
    """Consume all messages from topic. Returns list of (key, value)."""
    group_config = config.copy()
    group_config["group.id"] = f"test-consumer-{uuid.uuid4()}"
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(topic)
    topic_metadata = cluster_metadata.topics[topic]
    consumer.assign(
        [
            TopicPartition(topic, i, OFFSET_BEGINNING)
            for i in topic_metadata.partitions.keys()
        ]
    )
    out = []
    eof_count = 0
    total_partitions = len(topic_metadata.partitions)
    while eof_count < total_partitions:
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            break
        if msg.error() and msg.error().code() == KafkaError._PARTITION_EOF:
            eof_count += 1
            continue
        if msg.error():
            err_msg = f"Kafka error: {msg.error()}"
            raise RuntimeError(err_msg)
        out.append((msg.key(), msg.value()))
    consumer.close()
    return out


def consume_all_with_headers(config, topic, timeout=10.0):
    """Consume all messages. Returns list of (key, value, headers)."""
    group_config = config.copy()
    group_config["group.id"] = f"test-consumer-{uuid.uuid4()}"
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(topic)
    topic_metadata = cluster_metadata.topics[topic]
    consumer.assign(
        [
            TopicPartition(topic, i, OFFSET_BEGINNING)
            for i in topic_metadata.partitions.keys()
        ]
    )
    out = []
    eof_count = 0
    total_partitions = len(topic_metadata.partitions)
    while eof_count < total_partitions:
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            break
        if msg.error() and msg.error().code() == KafkaError._PARTITION_EOF:
            eof_count += 1
            continue
        if msg.error():
            err_msg = f"Kafka error: {msg.error()}"
            raise RuntimeError(err_msg)
        out.append((msg.key(), msg.value(), msg.headers() or []))
    consumer.close()
    return out


# ===================================================================
# Category 1: Wheel Smoke Tests
# ===================================================================


def test_import_all_modules():
    """All public modules are importable from installed wheel."""
    modules = [
        "bytewax",
        "bytewax.operators",
        "bytewax.dataflow",
        "bytewax.recovery",
        "bytewax.testing",
        "bytewax.tracing",
        "bytewax.inputs",
        "bytewax.outputs",
        "bytewax.errors",
        "bytewax.visualize",
        "bytewax._metrics",
    ]
    for mod_name in modules:
        importlib.import_module(mod_name)

    bw = importlib.import_module("bytewax")
    assert hasattr(bw, "__version__")


def test_basic_pipeline():
    """Map + filter pipeline using TestingSource/TestingSink."""
    inp = [1, 2, 3, 4, 5]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("add1", s, lambda x: x + 1)
    s = op.filter("gt3", s, lambda x: x > 3)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == [4, 5, 6]


def test_basic_pipeline_cluster():
    """Same pipeline via cluster_main with 2 workers."""
    inp = [1, 2, 3, 4, 5]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("add1", s, lambda x: x + 1)
    s = op.filter("gt3", s, lambda x: x > 3)
    op.output("out", s, TestingSink(out))

    cluster_main(flow, [], 0, worker_count_per_proc=2)
    assert sorted(out) == [4, 5, 6]


def test_all_operator_types():
    """Chain map, flat_map, filter, key_on, stateful_map in one pipeline."""

    def keep_max(max_val, new_val):
        if max_val is None:
            max_val = 0
        max_val = max(max_val, new_val)
        return (max_val, max_val)

    inp = [1, 2, 3, 4, 5, 6]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("double", s, lambda x: x * 2)
    s = op.flat_map("dup", s, lambda x: [x, x + 100])
    s = op.filter("lt200", s, lambda x: x < 200)
    s = op.key_on("key", s, lambda x: "even" if x % 2 == 0 else "odd")
    s = op.stateful_map("max", s, keep_max)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert len(out) > 0
    for item in out:
        assert isinstance(item, tuple)
        assert len(item) == 2
        assert item[0] in ("even", "odd")


# ===================================================================
# Category 7: Untested Operators
# ===================================================================


def test_redistribute():
    """Redistribute passes all items through without loss."""
    inp = list(range(20))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.redistribute("redist", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(inp)


def test_flat_map_value():
    """flat_map_value expands values while preserving keys."""
    inp = [("k1", "a b c"), ("k2", "d"), ("k1", "e f")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map_value("split", s, str.split)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("k1", "a"),
            ("k1", "b"),
            ("k1", "c"),
            ("k2", "d"),
            ("k1", "e"),
            ("k1", "f"),
        ]
    )


def test_stateful_batch():
    """StatefulBatchLogic accumulates state across batches."""

    class RunningSum(StatefulBatchLogic):
        def __init__(self, resume_state):
            self._sum = resume_state if resume_state is not None else 0

        def on_batch(self, values):
            for v in values:
                self._sum += v
            return [self._sum], self.RETAIN

        def snapshot(self):
            return self._sum

    inp = [("a", 1), ("a", 2), ("b", 10), ("a", 3), ("b", 20)]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_batch("sum", s, RunningSum)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    a_vals = [v for k, v in out if k == "a"]
    b_vals = [v for k, v in out if k == "b"]
    # Last emitted value for each key must be the total.
    assert a_vals[-1] == 6
    assert b_vals[-1] == 30


# ===================================================================
# Category 4: Windowed Aggregation
# ===================================================================


def test_tumbling_window_event_time():
    """Tumbling window groups items into fixed-duration buckets."""
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp = [
        # Window 1: [0s, 10s)
        ("ALL", (align_to + timedelta(seconds=1), "a")),
        ("ALL", (align_to + timedelta(seconds=3), "b")),
        ("ALL", (align_to + timedelta(seconds=8), "c")),
        # Window 2: [10s, 20s)
        ("ALL", (align_to + timedelta(seconds=11), "d")),
        ("ALL", (align_to + timedelta(seconds=15), "e")),
    ]
    out = []

    clock = EventClock(
        lambda x: x[0],
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = TumblingWindower(
        length=timedelta(seconds=10),
        align_to=align_to,
    )

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    win = collect_window("cw", s, clock, windower)
    s = op.key_rm("unkey", win.down)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    window_values = [sorted(v for _ts, v in items) for _wid, items in out]
    assert sorted(window_values) == sorted([["a", "b", "c"], ["d", "e"]])


def test_session_window():
    """Session window groups items within gap duration."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp = [
        # Session 1: events at 0s, 2s, 4s (gap < 5s)
        ("user1", (base, "a")),
        ("user1", (base + timedelta(seconds=2), "b")),
        ("user1", (base + timedelta(seconds=4), "c")),
        # 10s gap → new session
        ("user1", (base + timedelta(seconds=14), "d")),
        ("user1", (base + timedelta(seconds=16), "e")),
    ]
    out = []

    clock = EventClock(
        lambda x: x[0],
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = SessionWindower(gap=timedelta(seconds=5))

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    win = collect_window("cw", s, clock, windower)
    s = op.key_rm("unkey", win.down)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    windows = [sorted(v for _ts, v in items) for _wid, items in out]
    assert sorted(windows) == sorted([["a", "b", "c"], ["d", "e"]])


def test_sliding_window():
    """Sliding window creates overlapping windows."""
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # length=10s, offset=5s → 5s overlap
    inp = [
        ("ALL", (align_to + timedelta(seconds=2), 1)),
        ("ALL", (align_to + timedelta(seconds=7), 2)),
        ("ALL", (align_to + timedelta(seconds=12), 3)),
    ]
    out = []

    clock = EventClock(
        lambda x: x[0],
        wait_for_system_duration=timedelta(seconds=0),
    )
    windower = SlidingWindower(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=align_to,
    )

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    win = fold_window(
        "fw",
        s,
        clock,
        windower,
        builder=list,
        folder=lambda acc, x: acc + [x[1]],
        merger=lambda a, b: a + b,
    )
    s = op.key_rm("unkey", win.down)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    all_values = []
    for _wid, items in out:
        all_values.extend(items)
    # Overlapping windows mean some values appear in multiple windows.
    assert len(all_values) > 3
    c = Counter(all_values)
    assert any(count > 1 for count in c.values())


# ===================================================================
# Category 6: Multi-Worker
# ===================================================================


def test_multi_worker_all_data_processed():
    """cluster_main with 2 workers processes all data without loss."""
    inp = list(range(100))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("inc", s, lambda x: x + 1)
    op.output("out", s, TestingSink(out))

    cluster_main(flow, [], 0, worker_count_per_proc=2)
    assert sorted(out) == list(range(1, 101))


def test_multi_worker_stateful_key_isolation():
    """Each key maintains independent state across workers."""

    def running_sum(total, val):
        if total is None:
            total = 0
        total += val
        return (total, total)

    inp = [
        ("a", 1),
        ("b", 10),
        ("a", 2),
        ("b", 20),
        ("a", 3),
        ("b", 30),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("sum", s, running_sum)
    op.output("out", s, TestingSink(out))

    cluster_main(flow, [], 0, worker_count_per_proc=2)
    a_vals = sorted([v for k, v in out if k == "a"])
    b_vals = sorted([v for k, v in out if k == "b"])
    assert a_vals == [1, 3, 6]
    assert b_vals == [10, 30, 60]


# ===================================================================
# Category 5: Recovery
# ===================================================================


def test_recovery_stateful_abort_resume(tmp_path):
    """Stateful operator resumes from checkpoint after abort."""
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))

    def running_sum(total, val):
        if total is None:
            total = 0
        total += val
        return (total, total)

    inp = [
        ("a", 1),
        ("a", 2),
        ("a", 3),
        TestingSource.ABORT(),
        ("a", 4),
        ("a", 5),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("sum", s, running_sum)
    op.output("out", s, TestingSink(out))

    # Run 1: processes 1,2,3 then aborts. Snapshots at epoch_interval=0.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert sorted(out) == [("a", 1), ("a", 3), ("a", 6)]

    # Run 2: resumes with state=6, processes 4,5.
    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert sorted(out) == [("a", 10), ("a", 15)]


def test_recovery_continuation_stateful(tmp_path):
    """Stateful operator with EOF-based continuation."""
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))

    def running_sum(total, val):
        if total is None:
            total = 0
        total += val
        return (total, total)

    inp = [
        ("a", 1),
        ("a", 2),
        TestingSource.EOF(),
        ("a", 3),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("sum", s, running_sum)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert sorted(out) == [("a", 1), ("a", 3)]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert sorted(out) == [("a", 6)]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == []


def test_recovery_rescale_workers(tmp_path):
    """Recovery works when changing worker count between runs."""
    init_db_dir(tmp_path, 3)
    recovery_config = RecoveryConfig(str(tmp_path))

    def running_sum(total, val):
        if total is None:
            total = 0
        total += val
        return (total, total)

    inp = [
        ("a", 1),
        ("b", 10),
        TestingSource.EOF(),
        ("a", 2),
        ("b", 20),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("sum", s, running_sum)
    op.output("out", s, TestingSink(out))

    # Run 1 with 2 workers.
    cluster_main(
        flow,
        [],
        0,
        epoch_interval=ZERO_TD,
        recovery_config=recovery_config,
        worker_count_per_proc=2,
    )
    assert sorted(out) == [("a", 1), ("b", 10)]

    # Run 2 with 1 worker (rescale down).
    out.clear()
    cluster_main(
        flow,
        [],
        0,
        epoch_interval=ZERO_TD,
        recovery_config=recovery_config,
        worker_count_per_proc=1,
    )
    assert sorted(out) == [("a", 3), ("b", 30)]


# ===================================================================
# Category 2: Kafka End-to-End
# ===================================================================


@needs_kafka
def test_kafka_roundtrip(tmp_topic_in, tmp_topic_out, kafka_config, producer):
    """Produce → KafkaSource → transform → KafkaSink → consume."""
    for i in range(10):
        producer.produce(
            tmp_topic_in,
            value=f"value-{i}".encode(),
            key=f"key-{i}".encode(),
        )
    producer.flush()

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER],
            [tmp_topic_in],
            tail=False,
            add_config=kafka_config,
        ),
    )

    def uppercase_value(msg):
        return KafkaSinkMessage(
            key=msg.key,
            value=msg.value.upper() if msg.value else msg.value,
            topic=tmp_topic_out,
        )

    s = op.map("transform", s, uppercase_value)
    op.output(
        "out",
        s,
        KafkaSink([KAFKA_BROKER], tmp_topic_out, add_config=kafka_config),
    )

    run_main(flow)

    result = consume_all(kafka_config, tmp_topic_out)
    assert len(result) == 10
    values = sorted([v for _k, v in result])
    expected = sorted([f"VALUE-{i}".encode() for i in range(10)])
    assert values == expected


@needs_kafka
def test_kafka_multi_partition(tmp_topic, kafka_config, producer):
    """Messages across 3 partitions are all consumed."""
    expected = []
    for p in range(3):
        for i in range(5):
            key = f"p{p}-{i}".encode()
            value = f"val-{p}-{i}".encode()
            producer.produce(tmp_topic, value=value, key=key, partition=p)
            expected.append((key, value))
    producer.flush()

    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER],
            [tmp_topic],
            tail=False,
            add_config=kafka_config,
        ),
    )
    s = op.map("kv", s, lambda msg: (msg.key, msg.value))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(expected)


@needs_kafka
def test_kafka_headers_preserved(tmp_topic_in, tmp_topic_out, kafka_config, producer):
    """Message headers survive roundtrip through Kafka."""
    headers = [("trace-id", b"abc123"), ("source", b"test")]
    producer.produce(
        tmp_topic_in,
        value=b"hello",
        key=b"k1",
        headers=headers,
    )
    producer.flush()

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER],
            [tmp_topic_in],
            tail=False,
            add_config=kafka_config,
        ),
    )
    s = op.map(
        "to_sink",
        s,
        lambda msg: KafkaSinkMessage(
            key=msg.key,
            value=msg.value,
            headers=msg.headers,
            topic=tmp_topic_out,
        ),
    )
    op.output(
        "out",
        s,
        KafkaSink([KAFKA_BROKER], tmp_topic_out, add_config=kafka_config),
    )

    run_main(flow)

    result = consume_all_with_headers(kafka_config, tmp_topic_out)
    assert len(result) == 1
    k, v, hdrs = result[0]
    assert k == b"k1"
    assert v == b"hello"
    assert sorted(hdrs) == sorted(headers)


@needs_kafka
def test_kafka_json_roundtrip(tmp_topic_in, tmp_topic_out, kafka_config, producer):
    """JSON data survives produce → consume → transform → produce → consume."""
    records = [{"id": i, "name": f"item-{i}", "value": i * 10} for i in range(5)]
    for r in records:
        producer.produce(
            tmp_topic_in,
            value=json.dumps(r).encode(),
            key=str(r["id"]).encode(),
        )
    producer.flush()

    def transform(msg):
        data = json.loads(msg.value.decode())
        data["value"] *= 2
        data["transformed"] = True
        return KafkaSinkMessage(
            key=msg.key,
            value=json.dumps(data).encode(),
            topic=tmp_topic_out,
        )

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER],
            [tmp_topic_in],
            tail=False,
            add_config=kafka_config,
        ),
    )
    s = op.map("transform", s, transform)
    op.output(
        "out",
        s,
        KafkaSink([KAFKA_BROKER], tmp_topic_out, add_config=kafka_config),
    )

    run_main(flow)

    result = consume_all(kafka_config, tmp_topic_out)
    assert len(result) == 5
    for _k, v in result:
        data = json.loads(v.decode())
        assert data["transformed"] is True
        orig = next(r for r in records if r["id"] == data["id"])
        assert data["value"] == orig["value"] * 2


# ===================================================================
# Category 3: Error Scenarios
# ===================================================================


@needs_kafka
def test_kafka_missing_topic_error(kafka_config):
    """KafkaSource with non-existent topic raises error."""
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER],
            ["nonexistent-topic-xyz-12345"],
            tail=False,
            add_config=kafka_config,
        ),
    )
    op.output("out", s, TestingSink(out))

    with raises(BytewaxRuntimeError):
        run_main(flow)


@needs_kafka
def test_kafka_bad_deserializer_error_stream(tmp_topic, kafka_config, producer):
    """Bad deserializer routes errors to .errs stream."""
    try:
        from bytewax.connectors.kafka.serde import PlainAvroDeserializer  # noqa: I001, PLC0415
    except ImportError:
        from pytest import skip  # noqa: I001, PLC0415

        skip("fastavro not installed")

    producer.produce(tmp_topic, value=b"not-avro-data", key=b"k1")
    producer.produce(tmp_topic, value=b"also-not-avro", key=b"k2")
    producer.flush()

    schema = (
        '{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}'
    )
    deser = PlainAvroDeserializer(schema)

    oks = []
    errs = []

    flow = Dataflow("test_df")
    kafka_out = kop.input(
        "inp",
        flow,
        brokers=[KAFKA_BROKER],
        topics=[tmp_topic],
        tail=False,
        add_config=kafka_config,
    )
    deser_out = kop.deserialize_value("deser", kafka_out.oks, deser)
    op.output("oks", deser_out.oks, TestingSink(oks))
    op.output("errs", deser_out.errs, TestingSink(errs))

    run_main(flow)

    assert len(oks) == 0
    assert len(errs) == 2


@needs_kafka
def test_kafka_sink_bad_broker():
    """KafkaSink with unreachable broker should fail or timeout."""
    inp = [KafkaSinkMessage(b"k1", b"v1")]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output(
        "out",
        s,
        KafkaSink(
            ["localhost:19999"],
            "some-topic",
            add_config={
                "delivery.timeout.ms": "3000",
                "message.timeout.ms": "3000",
            },
        ),
    )

    # Producer may buffer without error, or may raise on flush.
    # Either outcome is acceptable; we verify it doesn't hang.
    try:
        run_main(flow)
    except Exception:
        pass


def test_backpressure_large_batch():
    """10k items process without data loss."""
    inp = list(range(10000))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("inc", s, lambda x: x + 1)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == list(range(1, 10001))


# ===================================================================
# Category 8: Metrics
# ===================================================================


def test_metrics_output():
    """generate_python_metrics returns valid prometheus text."""
    inp = list(range(10))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("inc", s, lambda x: x + 1)
    op.output("out", s, TestingSink(out))

    run_main(flow)

    metrics = generate_python_metrics()
    assert isinstance(metrics, str)
    assert len(metrics) > 0
