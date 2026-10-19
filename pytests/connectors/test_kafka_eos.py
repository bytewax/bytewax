"""Crash, drain and resume tests for `KafkaEosSink` against a broker.

Each test runs a stateful pipeline into a Kafka topic, crashes or drains
it, resumes, and reads the topic back. The topic must hold every pipeline
result exactly once, in per-key order. The recovery lock and multi-process
behaviour are in `test_kafka_eos_scale.py`, and the build checks in
`test_kafka_eos_unit.py`.

Needs a broker: set `TEST_KAFKA_BROKER`.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zlib
from collections import Counter
from concurrent.futures import Future
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, NamedTuple, Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, eos
from bytewax.connectors.kafka.eos import KafkaEosSink
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.run import request_graceful_drain
from bytewax.testing import TestingSource, cluster_main, run_main
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from pytest import fixture, mark, param, raises

pytestmark = mark.skipif(
    not os.environ.get("TEST_KAFKA_BROKER"),
    reason="Set `TEST_KAFKA_BROKER` env var (non-empty) to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")

# A Kafka broker on Windows cannot delete a topic, or the head of a log: it
# cannot rename the memory-mapped segment files (KAFKA-1194), so it takes its
# log dir offline and shuts down, and every later test fails. Topic names are
# unique, so no test needs to clean up; only a test that must delete is
# skipped there.
needs_topic_deletion = mark.skipif(
    sys.platform == "win32",
    reason="a Kafka broker on Windows cannot delete a topic: it cannot rename "
    "the memory-mapped segment files (KAFKA-1194), so it shuts down",
)
needs_delete_records_cli = mark.skipif(
    sys.platform == "win32",
    reason="simulates retention with kafka-delete-records.sh, a bash script "
    "Windows cannot run, and a Kafka broker on Windows cannot delete log "
    "segments either (KAFKA-1194)",
)

ITEMS = list(range(1, 61))
NUM_KEYS = 4
EPOCH_TD = timedelta(seconds=0.05)
SNAPSHOT_TD = EPOCH_TD * 3
# Longer than an epoch, so each item closes its own epoch and the
# input replays deterministically across executions.
SLOW_SECONDS = 0.08
# Crash on this value, so a window is still open at the crash.
MID_WINDOW = 21
# Crash on the first value after the snapshot at epoch 6, before the
# window that boundary closed is published.
AFTER_BOUNDARY = 7
DRAIN_AT = 25
# Crash late in the input, after several windows have been published.
LATE_CRASH = 45
# Pause after `GAP_AT` items for several epochs, so a snapshot boundary
# closes with no items in it, then crash just after the pause.
GAP_AT = 9
GAP_TD = EPOCH_TD * 10
AFTER_GAP = 11

Pair = Tuple[str, str]


@fixture
def eos_topic(request):
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", request.node.name)
    return _create_topic(f"eos-{safe_name}-{uuid.uuid4()}")


class _Placed(NamedTuple):
    """A topic, the partition all items go to, and a key that routes there."""

    topic: str
    partition: int
    route: str


# The placements the crash tests run on: the one-partition topic,
# and partitions other than 0 of a four-partition topic.
PLACEMENTS = [
    param((1, 0), id="one-partition"),
    param((4, 1), id="partition-1-of-4"),
    param((4, 3), id="partition-3-of-4"),
]


def _route_to(partition: int, partitions: int) -> str:
    """A routing key that `KafkaEosSink` sends to `partition` of `partitions`."""
    index = 0
    while True:
        key = f"route-{index}"
        if zlib.crc32(key.encode("utf-8")) % partitions == partition:
            return key
        index += 1


@fixture
def placed(request) -> _Placed:
    """A topic of `partitions` partitions; `request.param` is that and `partition`."""
    partitions, partition = request.param
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", request.node.name)
    topic = _create_topic(f"eos-{safe_name}-{uuid.uuid4()}", partitions=partitions)
    return _Placed(topic, partition, _route_to(partition, partitions))


def _await_leaders(name: str, total: int) -> None:
    """Wait until `name` lists `total` partitions, each with a leader.

    Right after a create or a delete, the broker may not list the topic yet,
    or may list it with no leader, so neither is an error here.
    """
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    deadline = time.monotonic() + 30.0
    while True:
        topic = client.list_topics(name, timeout=10.0).topics.get(name)
        if topic is not None and topic.error is None:
            partitions = topic.partitions
            if len(partitions) == total and all(
                p.leader >= 0 for p in partitions.values()
            ):
                return
        if time.monotonic() > deadline:
            err = f"topic `{name}` did not reach {total} partitions in time"
            raise RuntimeError(err)
        time.sleep(0.2)


def _grow_topic(name: str, total: int) -> None:
    """Add partitions until `name` has `total`, and wait for their leaders."""
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for fut in client.create_partitions(
        [NewPartitions(name, total)], operation_timeout=10.0
    ).values():
        fut.result()
    _await_leaders(name, total)


def _watermarks(topic: str, partition: int = 0) -> Tuple[int, int]:
    """The low and high watermarks of `partition`."""
    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BROKER, "group.id": f"eos-test-{uuid.uuid4()}"}
    )
    try:
        return consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0
        )
    finally:
        consumer.close()


def _high(topic: str, partition: int) -> int:
    """The log end offset of `partition`."""
    return _watermarks(topic, partition)[1]


def _create_topic(
    name: str, config: Optional[Dict[str, str]] = None, partitions: int = 1
) -> str:
    """Create `name`, and wait until its partitions have leaders.

    The sink lists the partitions when it is built, and a topic with no
    leaders yet is refused there, so the wait is part of the precondition.
    """
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for fut in client.create_topics(
        [NewTopic(name, partitions, replication_factor=1, config=config or {})],
        operation_timeout=5.0,
    ).values():
        fut.result()
    _await_leaders(name, partitions)
    return name


def _recreate_topic(name: str) -> None:
    """Delete `name`, wait until it is gone, and create it again empty."""
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for fut in client.delete_topics([name], operation_timeout=10.0).values():
        fut.result()
    deadline = time.monotonic() + 30.0
    while name in client.list_topics(timeout=5.0).topics:
        if time.monotonic() > deadline:
            err = f"topic `{name}` was not deleted in time"
            raise RuntimeError(err)
        time.sleep(0.2)
    # A ZooKeeper-mode broker keeps a deleted topic marked for deletion for
    # a moment after metadata stops listing it, and refuses to create it
    # again until then.
    create_deadline = time.monotonic() + 30.0
    while True:
        try:
            _create_topic(name)
            return
        except KafkaException as ex:
            code = ex.args[0].code() if ex.args else None
            if code != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            if time.monotonic() > create_deadline:
                raise
            time.sleep(0.2)


def _expected() -> List[Pair]:
    running: Dict[str, int] = {}
    out = []
    for item in ITEMS:
        key = f"key{item % NUM_KEYS}"
        running[key] = running.get(key, 0) + item
        out.append((key, f"{item}:{running[key]}"))
    return out


def _consume(topic: str, partition: int = 0) -> List[Pair]:
    """Every message on `partition` of the topic, from the beginning."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"eos-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0
        )
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        out: List[Pair] = []
        deadline = time.monotonic() + 30.0
        while len(out) < high - low and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            out.append((msg.key().decode(), msg.value().decode()))
        assert len(out) == high - low
        return out
    finally:
        consumer.close()


def _assert_exactly_once(consumed: List[Pair], expected: List[Pair]) -> None:
    assert Counter(consumed) == Counter(expected)
    keys = {key for key, _ in expected}
    for key in keys:
        assert [v for k, v in consumed if k == key] == [
            v for k, v in expected if k == key
        ]


def _slow_running_sum(state, item):
    time.sleep(SLOW_SECONDS)
    total = (state or 0) + item
    return (total, (item, total))


def _keyed(item):
    return (f"key{item % NUM_KEYS}", item)


def _encode(kv):
    key, (item, total) = kv
    return KafkaSinkMessage(
        key=key.encode(), value=f"{item}:{total}".encode(), topic=None
    )


def _source():
    return TestingSource(ITEMS)


def _crash_on(value: int):
    """A map that raises the first time it sees `value`.

    A raising map is a real crash: the execution fails, and the store
    keeps only what was durable before it. `TestingSource.ABORT()` is
    a graceful stop instead, so it is not used here.

    """
    fired = {"done": False}

    def crash(item):
        if item == value and not fired["done"]:
            fired["done"] = True
            msg = f"simulated crash on {value}"
            raise RuntimeError(msg)
        return item

    return crash


def _root_cause(exc: BaseException) -> str:
    """Message of the innermost exception in a runtime error chain."""
    while exc.__cause__ is not None:
        exc = exc.__cause__
    return str(exc)


def _flow(
    source, topic: str, *, eos: bool, on_item=None, route: str = "eos", **sink_kwargs
) -> Dataflow:
    flow = Dataflow("eos_df")
    s = op.input("inp", flow, source)
    if on_item is not None:
        s = op.map("hook", s, on_item)
    s = op.map("key", s, _keyed)
    s = op.stateful_map("sum", s, _slow_running_sum)
    msgs = op.map("encode", s, _encode)
    if eos:
        routed = op.map("route", msgs, lambda m: (route, m))
        op.output("out", routed, KafkaEosSink([KAFKA_BROKER], topic, **sink_kwargs))
    else:
        op.output("out", msgs, KafkaSink([KAFKA_BROKER], topic))
    return flow


def _recovery(path) -> RecoveryConfig:
    """Recovery config for `path`, initialised on first use only."""
    path.mkdir(parents=True, exist_ok=True)
    if not any(path.iterdir()):
        init_db_dir(path, 1)
    return RecoveryConfig(str(path), snapshot_interval=SNAPSHOT_TD)


def _run(flow: Dataflow, recovery_config: RecoveryConfig, workers: int = 1) -> None:
    if workers == 1:
        run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    else:
        cluster_main(
            flow,
            [],
            0,
            epoch_interval=EPOCH_TD,
            recovery_config=recovery_config,
            worker_count_per_proc=workers,
        )


def _run_crashing(
    flow: Dataflow, recovery_config: RecoveryConfig, workers: int = 1
) -> str:
    """Run an execution expected to crash; return the root cause."""
    with raises(RuntimeError) as excinfo:
        _run(flow, recovery_config, workers)
    return _root_cause(excinfo.value)


def _produce_raw(topic: str, records: List[Tuple[bytes, List[Tuple[str, bytes]]]]):
    """Write messages with explicit headers, bypassing the sink."""
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    for value, headers in records:
        producer.produce(topic, value=value, headers=headers)
    assert producer.flush(30.0) == 0


def _positions(topic: str, partition: int = 0) -> List[Tuple[int, int, int]]:
    """`(offset, window, seq)` of every message on `partition`, in order."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"eos-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0
        )
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        out: List[Tuple[int, int, int]] = []
        deadline = time.monotonic() + 30.0
        while len(out) < high - low and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            found = dict(msg.headers())
            out.append((msg.offset(), int(found["eos-window"]), int(found["eos-seq"])))
        assert len(out) == high - low
        return out
    finally:
        consumer.close()


def _delete_records_before(topic: str, offset: int) -> None:
    """Remove the head of partition 0 below `offset`, as retention would.

    Drives `kafka-delete-records.sh` from the local Kafka install that
    `scripts/ci-kafka.sh` downloads. Set `KAFKA_DIR` if it lives elsewhere.
    """
    kafka_dir = Path(
        os.environ.get("KAFKA_DIR", Path(__file__).resolve().parents[2] / ".kafka")
    )
    spec = {
        "partitions": [{"topic": topic, "partition": 0, "offset": offset}],
        "version": 1,
    }
    with tempfile.TemporaryDirectory() as tmp:
        spec_path = Path(tmp) / "delete-records.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        subprocess.run(
            [
                str(kafka_dir / "bin" / "kafka-delete-records.sh"),
                "--bootstrap-server",
                KAFKA_BROKER,
                "--offset-json-file",
                str(spec_path),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )


class _StaleLowConsumer(Consumer):
    """Reports the low watermark as it was before retention moved it.

    The build reads the watermarks, then retention moves the low
    watermark, then the build reads the tail. This reproduces that order.
    """

    def get_watermark_offsets(self, partition, timeout=None, cached=False):
        _low, high = super().get_watermark_offsets(
            partition, timeout=timeout, cached=cached
        )
        return 0, high


@mark.parametrize("placed", PLACEMENTS, indirect=True)
def test_eos_happy_path(tmp_path, placed):
    """A clean run publishes every result, and a second run adds nothing."""
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), placed.topic, eos=True, route=placed.route)

    # The run completed, so every recovery write is durable and `close()`
    # publishes the window left open at end of input.
    _run(flow, recovery_config)
    _assert_exactly_once(_consume(placed.topic, placed.partition), _expected())

    _run(flow, recovery_config)
    _assert_exactly_once(_consume(placed.topic, placed.partition), _expected())


@mark.parametrize("placed", PLACEMENTS, indirect=True)
def test_eos_crash_mid_window(tmp_path, placed):
    """a crash mid-window resumes to exactly once."""
    recovery_config = _recovery(tmp_path)
    flow = _flow(
        _source(),
        placed.topic,
        eos=True,
        on_item=_crash_on(MID_WINDOW),
        route=placed.route,
    )

    assert "simulated crash" in _run_crashing(flow, recovery_config)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    _assert_exactly_once(_consume(placed.topic, placed.partition), _expected())


@mark.parametrize("placed", PLACEMENTS, indirect=True)
def test_eos_crash_during_publish(tmp_path, placed, monkeypatch):
    """Crash partway through publishing a window.

    `BYTEWAX_EOS_CRASH_AFTER=k` is a test-only hook. The sink raises
    after flushing `k` produced messages of a publish, so `k` messages
    are in the topic and the rest of that window is not. The resume
    must skip those `k` and publish the rest, with no duplicates.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), placed.topic, eos=True, route=placed.route)

    monkeypatch.setenv("BYTEWAX_EOS_CRASH_AFTER", "2")
    assert "BYTEWAX_EOS_CRASH_AFTER" in _run_crashing(flow, recovery_config)
    # The two flushed messages are visible in the topic at crash time.
    assert len(_consume(placed.topic, placed.partition)) == 2
    monkeypatch.delenv("BYTEWAX_EOS_CRASH_AFTER")

    _run(flow, recovery_config)
    _run(flow, recovery_config)

    _assert_exactly_once(_consume(placed.topic, placed.partition), _expected())


@mark.parametrize("placed", PLACEMENTS, indirect=True)
def test_eos_crash_with_messages_unconfirmed_resumes_exactly_once(
    tmp_path, placed, monkeypatch
):
    """a crash after produce, before the delivery reports are taken.

    The hook raises after two messages are produced. Its flush is replaced by a
    wait until those two messages are in the topic, so the flush never takes
    their delivery reports. The crashed execution leaves the last snapshot's
    state, which lists the window as pending, and the topic holds the two
    messages. The resume must skip them and publish the rest, with no
    duplicates.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), placed.topic, eos=True, route=placed.route)
    holding = {"on": True}

    def wait_for_landing_without_reports(self, timeout=None):
        if holding["on"]:
            _await_records(placed.topic, placed.partition, 2)

    monkeypatch.setattr(eos._EosPartition, "_flush", wait_for_landing_without_reports)
    monkeypatch.setenv("BYTEWAX_EOS_CRASH_AFTER", "2")
    assert "BYTEWAX_EOS_CRASH_AFTER" in _run_crashing(flow, recovery_config)
    holding["on"] = False
    monkeypatch.delenv("BYTEWAX_EOS_CRASH_AFTER")
    assert len(_consume(placed.topic, placed.partition)) == 2

    _run(flow, recovery_config)
    _run(flow, recovery_config)

    _assert_exactly_once(_consume(placed.topic, placed.partition), _expected())


def _await_records(topic: str, partition: int, count: int) -> None:
    """Wait until `partition` holds `count` messages, without reading reports."""
    deadline = time.monotonic() + 30.0
    while len(_positions(topic, partition)) < count:
        if time.monotonic() > deadline:
            msg = f"`{topic}` never held {count} messages"
            raise AssertionError(msg)
        time.sleep(0.05)


def test_eos_crash_right_after_boundary(tmp_path, eos_topic):
    """Crash after a snapshot, before the window it closed is published.

    The window closed by the boundary at epoch 6 stays pending. The next
    execution must publish it exactly once, whether the crash left it
    durable or not.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True, on_item=_crash_on(AFTER_BOUNDARY))

    _run_crashing(flow, recovery_config)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    _assert_exactly_once(_consume(eos_topic), _expected())


def test_eos_graceful_drain_handoff(tmp_path, eos_topic):
    """a drain hands off; each window is published once."""
    recovery_config = _recovery(tmp_path)
    drained = {"done": False}

    def drain_once(item):
        if item == DRAIN_AT and not drained["done"]:
            drained["done"] = True
            request_graceful_drain()
        return item

    flow = _flow(_source(), eos_topic, eos=True, on_item=drain_once)

    _run(flow, recovery_config)
    # The drain ends the execution early, so the topic is still partial.
    assert len(_consume(eos_topic)) < len(ITEMS)

    _run(flow, recovery_config)
    _run(flow, recovery_config)

    _assert_exactly_once(_consume(eos_topic), _expected())


def test_eos_eof_then_follow_up_publishes_once(tmp_path, eos_topic):
    """The window open at end of input publishes once, whatever follows."""
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)

    _run(flow, recovery_config)
    _assert_exactly_once(_consume(eos_topic), _expected())

    # Follow-up runs over the same store must not publish again.
    _run(flow, recovery_config)
    _assert_exactly_once(_consume(eos_topic), _expected())


def test_plain_sink_duplicates_for_contrast(tmp_path, eos_topic):
    """The same mid-window crash with the ordinary `KafkaSink`.

    The plain sink produces as items arrive, so the resume replays the
    items after the last snapshot and produces them a second time.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=False, on_item=_crash_on(MID_WINDOW))

    _run_crashing(flow, recovery_config)
    _run(flow, recovery_config)

    consumed = _consume(eos_topic)
    duplicated = [pair for pair, count in Counter(consumed).items() if count > 1]
    assert duplicated


def _gapped_source():
    """`ITEMS` with an empty stretch of epochs after `GAP_AT` items."""
    head = ITEMS[:GAP_AT]
    tail = ITEMS[GAP_AT:]
    return TestingSource(
        head + [TestingSource.PAUSE(for_duration=GAP_TD)] + tail, batch_size=1
    )


def test_eos_replay_with_different_batch_size(tmp_path, eos_topic):
    """The replay groups items differently from the crashed execution.

    The first execution takes one item per batch and crashes mid-window.
    The resumed executions take seven items per batch, so the replayed
    epochs hold different items. Items inside one epoch can reach the
    sink in a different order, so only per-key order is checked, along
    with exactly-once. The bytes published before the crash must come
    back unchanged, as an exact prefix of the final topic.

    """
    recovery_config = _recovery(tmp_path)
    crashed = _flow(
        TestingSource(ITEMS, batch_size=1),
        eos_topic,
        eos=True,
        on_item=_crash_on(MID_WINDOW),
    )
    assert "simulated crash" in _run_crashing(crashed, recovery_config)
    published_before = _consume(eos_topic)

    resumed = _flow(TestingSource(ITEMS, batch_size=7), eos_topic, eos=True)
    _run(resumed, recovery_config)
    _run(resumed, recovery_config)

    final = _consume(eos_topic)
    assert final[: len(published_before)] == published_before
    _assert_exactly_once(final, _expected())


def test_eos_two_workers_crash_and_resume(tmp_path, eos_topic):
    """The happy path and a mid-window crash, with two workers."""
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True, on_item=_crash_on(MID_WINDOW))

    assert "simulated crash" in _run_crashing(flow, recovery_config, workers=2)
    _run(flow, recovery_config, workers=2)
    _run(flow, recovery_config, workers=2)

    _assert_exactly_once(_consume(eos_topic), _expected())


def test_eos_empty_boundary_then_crash(tmp_path, eos_topic):
    """Items, an empty boundary epoch, then a crash, then a resume.

    The pause makes the snapshot boundary inside it close with no items.
    The crash after the pause must not lose or repeat anything.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(
        _gapped_source(),
        eos_topic,
        eos=True,
        on_item=_crash_on(AFTER_GAP),
    )

    assert "simulated crash" in _run_crashing(flow, recovery_config)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    assert _consume(eos_topic) == _expected()


def test_eos_fresh_store_on_used_topic_is_refused(tmp_path, eos_topic):
    """A fresh store must not adopt a topic another store wrote.

    The store never produced the topic's windows, so resuming it would
    skip them all. The build must fail and leave the topic untouched.

    """
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, _recovery(tmp_path / "used"))
    _run(flow, _recovery(tmp_path / "used"))
    used = _consume(eos_topic)
    assert used == _expected()

    with raises(RuntimeError) as excinfo:
        _run(flow, _recovery(tmp_path / "fresh"))
    assert eos_topic in _root_cause(excinfo.value)
    assert _consume(eos_topic) == used


def test_eos_restored_backup_store_is_refused(tmp_path, eos_topic):
    """A store restored from an older backup must not resume the topic.

    The backup is taken after a crash, so its window counter is behind
    the windows the finishing execution publishes. Restoring it leaves
    the topic ahead of the store.

    """
    flow = _flow(_source(), eos_topic, eos=True)
    store = tmp_path / "store"
    assert "simulated crash" in _run_crashing(
        _flow(_source(), eos_topic, eos=True, on_item=_crash_on(MID_WINDOW)),
        _recovery(store),
    )
    backup = tmp_path / "backup"
    shutil.copytree(store, backup)
    _run(flow, _recovery(store))
    _run(flow, _recovery(store))
    assert _consume(eos_topic)

    with raises(RuntimeError) as excinfo:
        _run(flow, _recovery(backup))
    assert eos_topic in _root_cause(excinfo.value)


@needs_topic_deletion
def test_eos_recreated_topic_refuses_stale_store(tmp_path, eos_topic):
    """A store from an earlier life of a topic must not resume it.

    Store A writes the topic and finishes. The topic is then deleted and
    created again under the same name, and store B writes the new topic.
    Store A still has sink state for that name, so its resume must refuse
    the topic, since another store wrote it.

    """
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, _recovery(tmp_path / "a"))
    _run(flow, _recovery(tmp_path / "a"))
    assert _consume(eos_topic) == _expected()

    _recreate_topic(eos_topic)
    _run(flow, _recovery(tmp_path / "b"))
    _run(flow, _recovery(tmp_path / "b"))
    assert _consume(eos_topic) == _expected()

    with raises(RuntimeError) as excinfo:
        _run(flow, _recovery(tmp_path / "a"))
    msg = _root_cause(excinfo.value)
    assert eos_topic in msg
    assert "Another KafkaEosSink store wrote the topic" in msg


def _raw_headers(window, seq):
    return [
        ("eos-store", b"other-store"),
        ("eos-window", str(window).encode()),
        ("eos-seq", str(seq).encode()),
    ]


def test_eos_fresh_store_refuses_a_topic_with_a_hole(tmp_path, eos_topic):
    """A fresh store refuses records it did not write, even with a gap in them.

    The records are written directly, with window 0 holding seq 0, 1, 3. A
    fresh store has no state to check them against, so the build refuses the
    topic before reading it, and leaves it untouched.

    """
    _produce_raw(
        eos_topic,
        [
            (b"0:1", _raw_headers(0, 0)),
            (b"0:2", _raw_headers(0, 1)),
            (b"0:4", _raw_headers(0, 3)),
        ],
    )
    before = _positions(eos_topic)
    with raises(RuntimeError) as excinfo:
        _run(_flow(_source(), eos_topic, eos=True), _recovery(tmp_path / "store"))
    assert "already holds 3 message(s)" in _root_cause(excinfo.value)
    assert _positions(eos_topic) == before


def test_eos_messages_carry_placement_headers(tmp_path, eos_topic):
    """Every message says where it sits: its window, seq and store."""
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"eos-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.assign([TopicPartition(eos_topic, 0, OFFSET_BEGINNING)])
        headers = []
        deadline = time.monotonic() + 30.0
        while len(headers) < len(ITEMS) and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            headers.append({key: value for key, value in msg.headers()})
    finally:
        consumer.close()

    assert len(headers) == len(ITEMS)
    stores = {h["eos-store"] for h in headers}
    assert len(stores) == 1
    windows = [int(h["eos-window"]) for h in headers]
    assert windows == sorted(windows)


@needs_delete_records_cli
def test_eos_resume_after_retention_removes_window_head(tmp_path, eos_topic):
    """Retention removes seq 0 of the last window, and the build resumes.

    Retention is not a delivery gap, so the window's visible seqs starting
    at 1 must not be refused as a hole at seq 0.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    positions = _positions(eos_topic)
    last = positions[-1][1]
    in_last = [pos for pos in positions if pos[1] == last]
    assert len(in_last) >= 2, "the last window needs two messages to lose its head"
    assert in_last[1][2] == 1
    cut = in_last[1][0]
    _delete_records_before(eos_topic, cut)

    _run(flow, recovery_config)
    assert _consume(eos_topic) == _expected()[cut:]


@needs_delete_records_cli
def test_eos_resume_after_retention_removed_every_record_publishes_nothing(
    tmp_path, eos_topic, monkeypatch
):
    """Retention removes every record after a clean end, and a resume publishes nothing.

    A clean end leaves its last windows in the state, so the resume restores
    them. Retention then removes every record, so the topic holds none of
    them, but its end is where the state left it. Each restored window was
    delivered before the end, so nothing is produced again: the topic does not
    grow, and the consumer sees the expected sequence it read before retention.

    """
    restored: List[int] = []
    real_build_part = eos.KafkaEosSink.build_part

    def record_restore(self, step_id, for_part, resume_state):
        if resume_state is not None:
            restored.append(sum(len(messages) for _n, messages in resume_state[3]))
        return real_build_part(self, step_id, for_part, resume_state)

    monkeypatch.setattr(eos.KafkaEosSink, "build_part", record_restore)
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, recovery_config)
    seen = _consume(eos_topic)
    assert seen == _expected()
    high = _high(eos_topic, 0)
    _delete_records_before(eos_topic, high)
    low, _ = _watermarks(eos_topic)
    assert low == high, "retention did not remove every record"

    _run(flow, recovery_config)
    assert sum(restored) > 0, "the resume restored no window, so it proves nothing"
    assert _high(eos_topic, 0) == high, "the resume produced messages again"
    assert _consume(eos_topic) == []


def test_eos_final_flush_failure_fails_the_completed_run(
    tmp_path, eos_topic, monkeypatch
):
    """A completed run whose final flush fails returns the error to its caller.

    The run has finished, so nothing is left to retry. The error must reach
    the caller, not only the log.

    """

    def failing_flush(self, timeout=None):
        msg = "injected final flush failure"
        raise RuntimeError(msg)

    monkeypatch.setattr(eos._EosPartition, "_flush", failing_flush)
    with raises(RuntimeError) as excinfo:
        _run(_flow(_source(), eos_topic, eos=True), _recovery(tmp_path))
    assert "injected final flush failure" in _root_cause(excinfo.value)


@needs_delete_records_cli
def test_eos_resume_when_retention_moves_low_watermark_after_the_read(
    tmp_path, eos_topic, monkeypatch
):
    """Retention runs between the watermark read and the tail read.

    The scan starts below the first available message. A scan that counts
    messages waits for the removed ones and times out. A scan bounded by
    offsets stops at the last message, and the build resumes.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    cut = 5
    _delete_records_before(eos_topic, cut)
    monkeypatch.setattr(eos, "Consumer", _StaleLowConsumer)
    monkeypatch.setattr(eos, "_TAIL_TIMEOUT_S", 5.0)

    _run(flow, recovery_config)
    assert _consume(eos_topic) == _expected()[cut:]


@needs_delete_records_cli
def test_eos_fresh_store_refuses_a_topic_that_retention_trimmed(tmp_path, eos_topic):
    """A fresh store refuses a trimmed topic with a gap, and leaves it untouched.

    Retention removes seq 0 of window 0, which leaves a gap at seq 2 as well.
    The store did not write these records, so the build refuses the topic
    before it reads them. Hole detection for a restored store is covered by
    the direct partition tests.

    """
    _produce_raw(
        eos_topic,
        [
            (b"0:1", _raw_headers(0, 0)),
            (b"0:2", _raw_headers(0, 1)),
            (b"0:4", _raw_headers(0, 3)),
        ],
    )
    _delete_records_before(eos_topic, 1)
    before = _positions(eos_topic)

    with raises(RuntimeError) as excinfo:
        _run(_flow(_source(), eos_topic, eos=True), _recovery(tmp_path / "store"))
    assert eos_topic in _root_cause(excinfo.value)
    assert _positions(eos_topic) == before


def _records(topic: str, partition: int = 0) -> List[Tuple[int, int, bytes, bytes]]:
    """`(window, seq, key, value)` of every message on `partition`, in order."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"eos-test-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0
        )
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
        out: List[Tuple[int, int, bytes, bytes]] = []
        deadline = time.monotonic() + 30.0
        while len(out) < high - low and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            found = dict(msg.headers())
            out.append(
                (
                    int(found["eos-window"]),
                    int(found["eos-seq"]),
                    msg.key(),
                    msg.value(),
                )
            )
        assert len(out) == high - low
        return out
    finally:
        consumer.close()


def test_compacted_topic_is_refused_at_build(tmp_path):
    """A compacted topic is refused at build, before anything is produced.

    Compaction removes messages from the middle of a window, which the
    resume check would read as a hole. The build names the topic and the
    policy, and produces nothing.

    """
    topic = _create_topic(
        f"eos-compacted-{uuid.uuid4()}", {"cleanup.policy": "compact"}
    )
    flow = _flow(_source(), topic, eos=True)
    with raises(RuntimeError) as excinfo:
        _run(flow, _recovery(tmp_path))
    msg = _root_cause(excinfo.value)
    assert topic in msg, msg
    assert "cleanup.policy='compact'" in msg, msg
    assert _consume(topic) == []


def _fake_admin(policy=None, failure=None):
    """An AdminClient stand-in whose `describe_configs` answers once."""

    class _Admin:
        def __init__(self, _config):
            pass

        def describe_configs(self, resources):
            future: Future = Future()
            if failure is not None:
                future.set_exception(failure)
            else:
                future.set_result({"cleanup.policy": SimpleNamespace(value=policy)})
            return {resource: future for resource in resources}

    return _Admin


@mark.parametrize(
    "policy, refused",
    [("delete", False), ("compact", True), ("compact,delete", True)],
)
def test_compaction_check_reads_cleanup_policy(monkeypatch, policy, refused):
    """the compaction check reads cleanup.policy and refuses `compact`."""
    admin = _fake_admin(policy=policy)({})
    if not refused:
        eos._check_not_compacted(admin, "eos-unused")
        return
    with raises(RuntimeError, match="includes compact") as excinfo:
        eos._check_not_compacted(admin, "eos-unused")
    assert "eos-unused" in str(excinfo.value)


def test_compaction_check_logs_and_continues_when_config_is_denied(monkeypatch, caplog):
    """A denied config read is a warning, not a refusal, so the build goes on.

    An ACL that denies `describe_configs` surfaces as a failed future. The
    check must log it and return, since it is not a hard dependency on
    admin rights.

    """
    denied = KafkaException("TOPIC_AUTHORIZATION_FAILED")
    admin = _fake_admin(failure=denied)({})
    with caplog.at_level(logging.WARNING, logger=eos.__name__):
        eos._check_not_compacted(admin, "eos-unused")
    warned = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("eos-unused" in r.getMessage() for r in warned), caplog.text


def _eos_record(store: str, window: int, seq: int):
    """Headers that place a record in `window` at `seq`, written by `store`."""
    return [
        ("eos-store", store.encode()),
        ("eos-window", str(window).encode()),
        ("eos-seq", str(seq).encode()),
    ]


def _placed(topic: str) -> List[Tuple[int, int, bytes]]:
    """`(window, seq, value)` of every message on the partition, in order.

    The keys are left out: records that `_produce_raw` writes carry no key,
    and the keys do not place a message, so they are not compared here.

    """
    return [(window, seq, value) for window, seq, _key, value in _records(topic)]


def _direct_state(store: str, counter: int, windows, end: int = 0, count: int = 1):
    """A recovery state for `_direct_partition`: `windows` is [(number, [value])]."""
    return (
        1,
        store,
        counter,
        [
            (number, [KafkaSinkMessage(key=b"k", value=v, topic=None) for v in values])
            for number, values in windows
        ],
        end,
        count,
    )


def _direct_partition(topic: str, resume_state):
    """Build the sink partition directly, from a hand-made recovery state.

    The topic is real, so the resume reads hit the broker.

    """
    return eos._EosPartition([KAFKA_BROKER], topic, {}, resume_state)


@fixture
def lock_holder(monkeypatch):
    """Run the direct partitions as a lock holder of an incomplete execution.

    Outside an execution nothing is flushed at close, so `close()` of a
    direct partition would not land what the build produced. A lock holder
    flushes what it produced, and publishes nothing new at close.

    """
    monkeypatch.setattr(eos, "execution_state", lambda: (True, False, True))


def test_eos_windows_are_contiguous_and_grouped(tmp_path, eos_topic):
    """every window holds seq 0..n with no holes, in ascending order.

    Each window's messages sit together in offset order, numbered from 0
    with no holes, and the windows follow one another in ascending
    `eos-window` order. Checked after a run and its finishing execution.
    A sink that skipped a seq, or interleaved two windows, would fail.

    """
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), eos_topic, eos=True)
    _run(flow, recovery_config)
    _run(flow, recovery_config)

    positions = _positions(eos_topic)
    windows = [window for _offset, window, _seq in positions]
    assert windows == sorted(windows)
    seqs_by_window: Dict[int, List[int]] = {}
    for _offset, window, seq in positions:
        seqs_by_window.setdefault(window, []).append(seq)
    for window, seqs in seqs_by_window.items():
        assert seqs == list(range(len(seqs))), f"window {window} has seqs {seqs}"
    assert sum(len(seqs) for seqs in seqs_by_window.values()) == len(ITEMS)


@mark.parametrize(
    "records, hole",
    [
        # Window 0 has seq 0 and 2, so seq 1 is missing, and window 1 is whole.
        ([(0, 0), (0, 2), (1, 0), (1, 1)], "hole at seq 1"),
        # Window 0 has seq 1 and 2 with no retention, so seq 0 is missing.
        ([(0, 1), (0, 2), (1, 0), (1, 1)], "hole at seq 0"),
    ],
)
def test_eos_resume_refuses_hole_in_restored_window(eos_topic, records, hole):
    """a gap inside a restored window that is not the last one is refused.

    The state holds windows 0 and 1. The topic's last window is whole, so
    the tail check passes, and the gap sits in window 0. Only the resume
    plan can see it, and it must refuse, naming the missing seq.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [(f"w{w}s{s}".encode(), _eos_record(store, w, s)) for w, s in records],
    )
    state = _direct_state(
        store, 2, [(0, [b"w0s0", b"w0s1", b"w0s2"]), (1, [b"w1s0", b"w1s1"])]
    )
    with raises(RuntimeError, match=hole):
        _direct_partition(eos_topic, state)


def test_eos_resume_refuses_topic_shorter_than_recorded_end(eos_topic):
    """a topic that ends before the state's recorded `end` is refused.

    The state says the store knew the topic to hold 5 messages, but the topic
    is empty, as after recreation. Resuming would republish windows the
    topic may have held, so the build refuses.

    """
    store = uuid.uuid4().hex
    state = _direct_state(store, 1, [(0, [b"a0"])], end=5)
    with raises(RuntimeError, match="ends at offset 0, before the offset 5"):
        _direct_partition(eos_topic, state)


def test_eos_resume_skips_window_present_whole_and_publishes_absent_window(
    eos_topic, lock_holder
):
    """a window the topic holds whole is skipped, an absent one is published.

    Window 0 is in the topic with its state bytes, so the resume must not
    produce it again. Window 1 is absent, so the resume publishes all of it.
    The published bytes must be the state's bytes, which the resume requires, since
    no input is replayed here.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [(b"a0", _eos_record(store, 0, 0)), (b"a1", _eos_record(store, 0, 1))],
    )
    state = _direct_state(store, 2, [(0, [b"a0", b"a1"]), (1, [b"b0", b"b1"])], end=0)
    _direct_partition(eos_topic, state).close()
    assert _placed(eos_topic) == [
        (0, 0, b"a0"),
        (0, 1, b"a1"),
        (1, 0, b"b0"),
        (1, 1, b"b1"),
    ]


def test_eos_resume_continues_partial_window_from_next_seq(eos_topic, lock_holder):
    """a partly present window resumes at its next seq, with the state's bytes.

    The topic holds seq 0 and 1 of a three-message window. The resume
    publishes seq 2 alone, and that message must carry the state's bytes,
    byte for byte, not a regenerated copy.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [(b"a0", _eos_record(store, 0, 0)), (b"a1", _eos_record(store, 0, 1))],
    )
    state = _direct_state(store, 1, [(0, [b"a0", b"a1", b"a2"])], end=0)
    _direct_partition(eos_topic, state).close()
    assert _placed(eos_topic) == [(0, 0, b"a0"), (0, 1, b"a1"), (0, 2, b"a2")]


@needs_delete_records_cli
def test_eos_retention_removing_a_whole_restored_window_publishes_nothing(
    eos_topic, lock_holder
):
    """retention removes a whole restored window, and nothing is republished.

    Window 0 was published at offsets 0 and 1, and then retention removed
    both, so the topic is empty at offset 2. The state's end is 0, the offset
    of window 0's first message, so the topic holds all of the state. Window 0
    was delivered before retention, so the build does not republish it.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [(b"a0", _eos_record(store, 0, 0)), (b"a1", _eos_record(store, 0, 1))],
    )
    _delete_records_before(eos_topic, 2)
    assert _consume(eos_topic) == []

    state = _direct_state(store, 1, [(0, [b"a0", b"a1"])], end=0)
    _direct_partition(eos_topic, state).close()
    assert _placed(eos_topic) == []


@needs_delete_records_cli
def test_eos_retention_removing_an_earlier_window_is_not_republished(
    eos_topic, lock_holder
):
    """retention removes window 0 whole, so the build does not republish it.

    Both windows were published after the state was taken, so the state holds
    both. Retention removes window 0 alone, which leaves window 1 whole. Windows
    are produced in order and the producer is gapless, so window 0 was
    delivered before window 1. Republishing it would put it after window 1, so
    the build leaves it out, and publishes nothing: the topic already holds
    the rest.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [
            (b"a0", _eos_record(store, 0, 0)),
            (b"a1", _eos_record(store, 0, 1)),
            (b"b0", _eos_record(store, 1, 0)),
            (b"b1", _eos_record(store, 1, 1)),
        ],
    )
    _delete_records_before(eos_topic, 2)
    state = _direct_state(store, 2, [(0, [b"a0", b"a1"]), (1, [b"b0", b"b1"])], end=0)
    _direct_partition(eos_topic, state).close()
    assert _placed(eos_topic) == [(1, 0, b"b0"), (1, 1, b"b1")]


@needs_delete_records_cli
def test_eos_retention_cut_inside_a_window_publishes_its_missing_tail(
    eos_topic, lock_holder
):
    """retention removes window 0 and the head of window 1.

    Window 1 has three messages, and the topic holds the first two. Retention
    removes offsets 0 to 2, which are window 0 and window 1's head, so window 1
    resumes at seq 2. Window 0 is not republished.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [
            (b"a0", _eos_record(store, 0, 0)),
            (b"a1", _eos_record(store, 0, 1)),
            (b"b0", _eos_record(store, 1, 0)),
            (b"b1", _eos_record(store, 1, 1)),
        ],
    )
    _delete_records_before(eos_topic, 3)
    state = _direct_state(
        store, 2, [(0, [b"a0", b"a1"]), (1, [b"b0", b"b1", b"b2"])], end=0
    )
    _direct_partition(eos_topic, state).close()
    assert _placed(eos_topic) == [(1, 1, b"b1"), (1, 2, b"b2")]


@needs_delete_records_cli
def test_eos_retention_hole_behind_a_window_the_topic_holds_is_refused(eos_topic):
    """a missing head behind a window the topic holds is still a hole.

    Retention removes window 0's seq 0, so window 0 keeps seq 1 and its head is
    gone, which the build accepts. Window 1 also lacks its seq 0, and that is
    not retention: retention removed only the head of window 0, and window 1's
    seq 0 would have come after window 0's messages. The build refuses it.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [
            (b"a0", _eos_record(store, 0, 0)),
            (b"a1", _eos_record(store, 0, 1)),
            (b"b1", _eos_record(store, 1, 1)),
        ],
    )
    _delete_records_before(eos_topic, 1)
    state = _direct_state(store, 2, [(0, [b"a0", b"a1"]), (1, [b"b0", b"b1"])], end=0)
    with raises(RuntimeError, match="hole at seq 0 in window 1"):
        _direct_partition(eos_topic, state)


def test_eos_window_lost_without_retention_before_a_whole_window_is_refused(eos_topic):
    """with no retention, a window the topic lacks before a whole one is refused.

    The topic holds window 1 whole and never held window 0. The low watermark
    is still at the state's end, so nothing was removed. The first message of
    the state is window 0's seq 0, and the topic holds window 1 at that offset,
    so the build refuses.

    """
    store = uuid.uuid4().hex
    _produce_raw(
        eos_topic,
        [(b"b0", _eos_record(store, 1, 0)), (b"b1", _eos_record(store, 1, 1))],
    )
    state = _direct_state(store, 2, [(0, [b"a0", b"a1"]), (1, [b"b0", b"b1"])], end=0)
    with raises(RuntimeError, match="hole at seq 0 in window 0"):
        _direct_partition(eos_topic, state)


@mark.parametrize(
    "before, after",
    [param(1, 4, id="one-to-four"), param(4, 8, id="four-to-eight")],
)
def test_eos_partition_count_fence_refuses_growth_twice_with_no_writes(
    tmp_path, before, after
):
    """a store refuses a topic that gained partitions, every time, unwritten.

    The first execution writes partition 0 of a topic with `before`
    partitions, so partition 0 holds a store state that records the count.
    The topic then grows to `after` partitions. Two executions on the same
    store are each refused, and the refusal names both counts. Neither
    writes to any partition, so every high watermark stays as it was.

    """
    name = f"eos-fence-{uuid.uuid4()}"
    _create_topic(name, partitions=before)
    _await_leaders(name, before)
    recovery_config = _recovery(tmp_path)
    flow = _flow(_source(), name, eos=True, route=_route_to(0, before))
    _run(flow, recovery_config)
    published = [_high(name, p) for p in range(before)]

    _grow_topic(name, after)
    causes = []
    for _attempt in range(2):
        with raises(RuntimeError) as excinfo:
            _run(flow, recovery_config)
        causes.append(_root_cause(excinfo.value))
    for cause in causes:
        assert (
            f"`{name}` with {before} partition{'s' if before > 1 else ''}, but "
            f"`{name}` has {after} partitions now"
        ) in cause, cause
        assert "Keys would move between partitions" in cause, cause
    assert [_high(name, p) for p in range(after)] == published + [0] * (after - before)


def test_eos_double_crash_publishes_once(tmp_path, eos_topic):
    """two crashes in a row, at different points, still publish once.

    The first crash is mid-window, and the resume crashes again later in the
    input. Each crash leaves a different state behind, and the last execution
    to finish must still publish every window exactly once.

    """
    recovery_config = _recovery(tmp_path)
    first = _flow(_source(), eos_topic, eos=True, on_item=_crash_on(MID_WINDOW))
    assert "simulated crash" in _run_crashing(first, recovery_config)
    second = _flow(_source(), eos_topic, eos=True, on_item=_crash_on(LATE_CRASH))
    assert "simulated crash" in _run_crashing(second, recovery_config)

    finish = _flow(_source(), eos_topic, eos=True)
    _run(finish, recovery_config)
    _run(finish, recovery_config)
    _assert_exactly_once(_consume(eos_topic), _expected())
