import os
import re
import time
import uuid
from concurrent.futures import wait
from datetime import timedelta
from typing import Any, Dict, List, Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
)
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import (
    TestingSink,
    TestingSource,
    cluster_main,
    poll_next_batch,
    run_main,
)
from confluent_kafka import (
    OFFSET_BEGINNING,
    OFFSET_END,
    TIMESTAMP_CREATE_TIME,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from pytest import fixture, mark, raises

pytestmark = mark.skipif(
    not os.environ.get("TEST_KAFKA_BROKER"),
    reason="Set `TEST_KAFKA_BROKER` env var (non-empty) to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")
config = {"bootstrap.servers": KAFKA_BROKER}
# Idempotent producers keep per-partition order even when a send is
# retried, e.g. right after a topic is created and its leader is still
# settling. Tests assert on that order.
producer_config = {**config, "enable.idempotence": "true"}
ZERO_TD = timedelta(seconds=0)


@fixture
def tmp_topic(request):
    client = AdminClient(config)
    # Parametrized test names contain `[...]`, which is not valid in a
    # Kafka topic name.
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", request.node.name)
    topic_name = f"pytest_{safe_name}_{uuid.uuid4()}"
    for fut in client.create_topics(
        # 3 partitions.
        [NewTopic(topic_name, 3)],
        operation_timeout=5.0,
    ).values():
        # Surface creation errors instead of timing out below.
        fut.result()
    # create_topics resolves once the broker has written topic
    # metadata; partition leader election is asynchronous, so a
    # producer that pins to a specific partition can otherwise hang
    # on delivery.timeout.ms (5 min default). Reproducible on
    # windows-latest with the local broker; rare on Linux/macOS.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        meta = client.list_topics(topic_name, timeout=5.0)
        topic = meta.topics.get(topic_name)
        if (
            topic is not None
            and topic.error is None
            and topic.partitions
            and all(p.leader >= 0 for p in topic.partitions.values())
        ):
            break
        time.sleep(0.1)
    else:
        msg = f"topic {topic_name!r} partitions never got a leader"
        raise RuntimeError(msg)
    yield topic_name
    if os.environ.get("TEST_KAFKA_SKIP_TOPIC_DELETE"):
        return
    wait(client.delete_topics([topic_name], operation_timeout=5.0).values())


tmp_topic1 = tmp_topic
tmp_topic2 = tmp_topic


def as_k_v(m: KafkaSourceMessage) -> Tuple[bytes, bytes]:
    return m.key, m.value


def test_input(tmp_topic1, tmp_topic2):
    topics = [tmp_topic1, tmp_topic2]
    producer = Producer(producer_config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key)
            inp.append((key, value))
    producer.flush()
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp", flow, KafkaSource([KAFKA_BROKER], topics, tail=False, add_config=config)
    )
    vals = op.map("vals", s, as_k_v)
    op.output("out", vals, TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_input_resume_state(tmp_topic):
    topics = [tmp_topic]
    partition = 0
    producer = Producer(producer_config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key, partition=partition)
            inp.append((key, value))
    producer.flush()

    inp = KafkaSource(
        [KAFKA_BROKER], topics, batch_size=1, tail=False, add_config=config
    )
    part = inp.build_part("test", f"{partition}-{tmp_topic}", None)
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-0", b"value-0-0")]
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-1", b"value-0-1")]
    resume_state = part.snapshot()
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    part.close()

    inp = KafkaSource([KAFKA_BROKER], topics, tail=False, add_config=config)
    part = inp.build_part("test", f"{partition}-{tmp_topic}", resume_state)
    assert part.snapshot() == resume_state
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    with raises(StopIteration):
        poll_next_batch(part)
    part.close()


def test_input_raises_on_topic_not_exist():
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource([KAFKA_BROKER], ["missing-topic"], tail=False, add_config=config),
    )
    op.output("out", s, TestingSink(out))

    with raises(BytewaxRuntimeError):
        run_main(flow)


def test_input_raises_on_str_brokers(tmp_topic):
    expect = "brokers must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource(KAFKA_BROKER, [tmp_topic], tail=False)


def test_input_raises_on_str_topics(tmp_topic):
    expect = "topics must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource([KAFKA_BROKER], tmp_topic, tail=False)


def consume_all(topic: str, count: int) -> List[Tuple[bytes, bytes]]:
    """Read `count` messages from all partitions of `topic`.

    Gives up after 30 seconds, so a short read fails the comparison
    instead of hanging the test.

    """
    group_config = config.copy()
    group_config["group.id"] = "BYTEWAX_UNIT_TEST"
    # Don't leave around a consumer group for this.
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(topic)
    topic_metadata = cluster_metadata.topics[topic]
    # Assign does not activate consumer grouping.
    consumer.assign(
        [
            TopicPartition(topic, i, OFFSET_BEGINNING)
            for i in topic_metadata.partitions.keys()
        ]
    )
    out: List[Tuple[bytes, bytes]] = []
    deadline = time.monotonic() + 30
    while len(out) < count and time.monotonic() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error() is not None and msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        assert msg.error() is None
        out.append((msg.key(), msg.value()))
    consumer.close()
    return out


def test_output(tmp_topic):
    flow = Dataflow("test_df")

    inp = [
        KafkaSinkMessage(b"key-0-0", b"value-0-0", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-1", b"value-0-2", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-2", b"value-0-2", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([KAFKA_BROKER], tmp_topic, add_config=config))

    run_main(flow)

    expected = list(map(as_k_v, inp))
    assert sorted(consume_all(tmp_topic, len(expected))) == sorted(expected)


PARTITIONS = 3


def produce_per_partition(
    topic: str, start: int, count: int
) -> Dict[int, List[Tuple[bytes, bytes]]]:
    """Produce `count` messages to each partition of `topic`.

    Keys and values encode the partition and a sequence number
    starting at `start`, so tests can check per-partition order.

    """
    producer = Producer(producer_config)
    produced: Dict[int, List[Tuple[bytes, bytes]]] = {}
    for part in range(PARTITIONS):
        for seq in range(start, start + count):
            key = f"p{part}-k{seq}".encode()
            value = f"p{part}-v{seq}".encode()
            producer.produce(
                topic, value, key, partition=part, headers=[("seq", str(seq).encode())]
            )
            produced.setdefault(part, []).append((key, value))
    producer.flush()
    return produced


ReadRecord = Tuple[str, int, int, bytes, bytes]
"""(topic, partition, offset, key, value) of a consumed message."""


def as_record(m: Any) -> ReadRecord:
    assert isinstance(m, KafkaSourceMessage)
    assert m.topic is not None
    assert m.partition is not None
    assert m.offset is not None
    assert m.key is not None
    assert m.value is not None
    return (m.topic, m.partition, m.offset, m.key, m.value)


def by_partition(out: List[ReadRecord]) -> Dict[int, List[ReadRecord]]:
    """Group read records by partition, keeping the order they arrived in."""
    parts: Dict[int, List[ReadRecord]] = {}
    for rec in out:
        parts.setdefault(rec[1], []).append(rec)
    return parts


def assert_read_exactly(
    out: List[ReadRecord],
    topic: str,
    produced: Dict[int, List[Tuple[bytes, bytes]]],
    first_offset: int,
) -> None:
    """Every produced message was read once, in order within its partition."""
    parts = by_partition(out)
    assert sorted(parts) == sorted(produced)
    for part, recs in parts.items():
        expected_offsets = list(range(first_offset, first_offset + len(produced[part])))
        assert [r[2] for r in recs] == expected_offsets, f"partition {part}"
        assert [(r[3], r[4]) for r in recs] == produced[part], f"partition {part}"
        assert {r[0] for r in recs} == {topic}


def build_read_flow(topic: str, out: List[ReadRecord], **source_kwargs) -> Dataflow:
    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource([KAFKA_BROKER], [topic], add_config=config, **source_kwargs),
    )
    recs = op.map("rec", s, as_record)
    op.output("out", recs, TestingSink(out))
    return flow


def test_input_reads_all_partitions_in_order(tmp_topic, entry_point):
    produced = produce_per_partition(tmp_topic, 0, 5)
    out: List[ReadRecord] = []

    entry_point(build_read_flow(tmp_topic, out, tail=False, batch_size=2))

    assert_read_exactly(out, tmp_topic, produced, first_offset=0)


def test_input_message_metadata(tmp_topic):
    # Use a recent timestamp: one older than the broker's retention
    # makes it delete the segment right away, and on Windows that
    # delete fails (the index file is still mapped) and takes the
    # broker's only log dir offline.
    timestamp_ms = int(time.time() * 1000) - 60_000
    producer = Producer(producer_config)
    producer.produce(
        tmp_topic,
        b"v",
        b"k",
        partition=1,
        headers=[("h1", b"a"), ("h2", b"b")],
        timestamp=timestamp_ms,
    )
    producer.flush()
    out: List[KafkaSourceMessage] = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource([KAFKA_BROKER], [tmp_topic], tail=False, add_config=config),
    )
    op.output("out", s, TestingSink(out))
    run_main(flow)

    (msg,) = out
    assert (msg.key, msg.value) == (b"k", b"v")
    assert msg.topic == tmp_topic
    assert msg.partition == 1
    assert msg.offset == 0
    assert msg.headers == [("h1", b"a"), ("h2", b"b")]
    assert msg.timestamp == (TIMESTAMP_CREATE_TIME, timestamp_ms)


def test_input_starting_offset_end_skips_existing(tmp_topic):
    produce_per_partition(tmp_topic, 0, 3)
    out: List[ReadRecord] = []

    run_main(build_read_flow(tmp_topic, out, tail=False, starting_offset=OFFSET_END))

    assert out == []


def test_input_continuation_reads_only_new_messages(tmp_topic, recovery_config):
    # Each run reads to the end of every partition and snapshots there;
    # the next run must pick up exactly where the last one stopped.
    out: List[ReadRecord] = []
    flow = build_read_flow(tmp_topic, out, tail=False)

    produced = produce_per_partition(tmp_topic, 0, 4)
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert_read_exactly(out, tmp_topic, produced, first_offset=0)

    out.clear()
    produced = produce_per_partition(tmp_topic, 4, 3)
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert_read_exactly(out, tmp_topic, produced, first_offset=4)

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == []


def test_input_resume_after_failure_loses_nothing(tmp_topic, recovery_config):
    produce_per_partition(tmp_topic, 0, 10)
    fail_on = (1, 6)  # (partition, offset)
    failing = [True]

    def check(rec: ReadRecord) -> ReadRecord:
        if failing[0] and (rec[1], rec[2]) == fail_on:
            msg = "boom"
            raise RuntimeError(msg)
        return rec

    out: List[ReadRecord] = []
    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [KAFKA_BROKER], [tmp_topic], tail=False, batch_size=1, add_config=config
        ),
    )
    s = op.map("rec", s, as_record)
    s = op.map("check", s, check)
    op.output("out", s, TestingSink(out))

    with raises(BytewaxRuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    first_run = list(out)
    assert fail_on not in {(r[1], r[2]) for r in first_run}

    out.clear()
    failing[0] = False
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    second_run = list(out)

    # Resuming may replay the epoch that was in flight when the
    # dataflow failed, but nothing may be lost, and the resumed run
    # reads each partition forward from a single point to the end.
    for part, recs in by_partition(second_run).items():
        offsets = [r[2] for r in recs]
        assert offsets == list(range(offsets[0], 10)), f"partition {part}"
    seen = {(r[1], r[2]) for r in first_run + second_run}
    assert seen == {(p, o) for p in range(PARTITIONS) for o in range(10)}


def test_input_resume_with_different_worker_counts(tmp_path, tmp_topic):
    # Recovery state is keyed by Kafka partition, so it must follow the
    # partition when it moves to a different worker on resume.
    init_db_dir(tmp_path, 3)
    recovery_config = RecoveryConfig(str(tmp_path))
    out: List[ReadRecord] = []
    flow = build_read_flow(tmp_topic, out, tail=False)

    def run(worker_count: int) -> None:
        cluster_main(
            flow,
            addresses=[],
            proc_id=0,
            epoch_interval=ZERO_TD,
            recovery_config=recovery_config,
            worker_count_per_proc=worker_count,
        )

    produced = produce_per_partition(tmp_topic, 0, 3)
    run(2)
    assert_read_exactly(out, tmp_topic, produced, first_offset=0)

    out.clear()
    produced = produce_per_partition(tmp_topic, 3, 3)
    run(3)
    assert_read_exactly(out, tmp_topic, produced, first_offset=3)

    out.clear()
    produced = produce_per_partition(tmp_topic, 6, 3)
    run(1)
    assert_read_exactly(out, tmp_topic, produced, first_offset=6)


def test_output_topic_from_message(tmp_topic1, tmp_topic2, entry_point):
    inp = [
        KafkaSinkMessage(f"k{i}".encode(), f"v{i}".encode(), topic=topic)
        for i, topic in enumerate([tmp_topic1, tmp_topic2] * 3)
    ]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([KAFKA_BROKER], None, add_config=config))
    entry_point(flow)

    for topic in [tmp_topic1, tmp_topic2]:
        expected = [as_k_v(m) for m in inp if m.topic == topic]
        assert sorted(consume_all(topic, len(expected))) == sorted(expected)


def test_operators_round_trip(tmp_topic1, tmp_topic2):
    producer = Producer(producer_config)
    for i in range(6):
        producer.produce(tmp_topic1, f"value-{i}".encode(), f"key-{i}".encode())
    producer.flush()
    errs: List[Any] = []

    flow = Dataflow("test_df")
    kinp = kop.input(
        "kafka_in",
        flow,
        brokers=[KAFKA_BROKER],
        topics=[tmp_topic1],
        tail=False,
        add_config=config,
    )
    op.output("input_errs", kinp.errs, TestingSink(errs))
    decoded = kop.deserialize(
        "decode",
        kinp.oks,
        key_deserializer=StringDeserializer(),
        val_deserializer=StringDeserializer(),
    )
    op.output("decode_errs", decoded.errs, TestingSink(errs))
    upper = op.map("upper", decoded.oks, lambda m: m._with_value(m.value.upper()))
    encoded = kop.serialize(
        "encode",
        upper,
        key_serializer=StringSerializer(),
        val_serializer=StringSerializer(),
    )
    kop.output(
        "kafka_out",
        encoded,
        brokers=[KAFKA_BROKER],
        topic=tmp_topic2,
        add_config=config,
    )
    run_main(flow)

    assert errs == []
    expected = [(f"key-{i}".encode(), f"VALUE-{i}".encode()) for i in range(6)]
    assert sorted(consume_all(tmp_topic2, len(expected))) == sorted(expected)
