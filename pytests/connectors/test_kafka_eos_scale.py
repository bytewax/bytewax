"""Exactly-once Kafka output across partitions, workers and executions.

Each keyed workload runs into a topic with several partitions, with a crash
and resumes, and every partition must hold each result exactly once. The
records of each key must sit in the partition its CRC-32 names, which is the
placement of librdkafka's `consistent` partitioner. A partition-count change
between executions is refused, and the refusal names both counts.

The multi-process tests run this file as a script, one process of the
cluster per run, and check the recovery lock: a successor cannot start while
its predecessor is alive, and a frozen predecessor finishes alone.

Needs a broker: set `TEST_KAFKA_BROKER`.

    test_eos_multi_partition_topic_exactly_once
    test_eos_four_topics_twenty_partitions
    test_eos_partitions_spread_over_workers_stay_exactly_once
    test_eos_delivery_failure_on_one_partition_then_resume_completes
    test_eos_partition_count_change_is_refused
    test_eos_multi_process_exactly_once
    test_eos_successor_refused_while_predecessor_is_frozen

See `_child_main`.

"""

import os
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import zlib
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSinkMessage, eos
from bytewax.connectors.kafka.eos import KafkaEosSink
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSource, cluster_main, run_main
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic
from pytest import mark, raises

pytestmark = mark.skipif(
    not os.environ.get("TEST_KAFKA_BROKER"),
    reason="Set `TEST_KAFKA_BROKER` env var (non-empty) to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")

ITEMS = list(range(1, 61))
EPOCH_TD = timedelta(seconds=0.05)
SNAPSHOT_TD = EPOCH_TD * 3
# Longer than an epoch, so each item closes its own epoch.
SLOW_SECONDS = 0.08
# Crash on this value, so a window is still open at the crash.
MID_WINDOW = 21
# Item at which a frozen predecessor holds mid-run, with its lock taken.
HOLD_AT = 30

NUM_KEYS = 4
WIDE_PARTITIONS = 50
WIDE_KEYS = 20
FOUR_TOPICS = 4
FOUR_PARTITIONS = 20
FOUR_KEYS = 8
GROWN_PARTITIONS = 4
# Messages the single-partition topic must hold before a process is killed.
GATE_MESSAGES = 6
GATE_TIMEOUT_S = 120.0


Pair = Tuple[str, str]
# (partition, key, value) of one message.
Record = Tuple[int, bytes, bytes]
Child = Tuple[subprocess.Popen, Path]


def _name(tag: str) -> str:
    return f"eos-scale-{tag}-{uuid.uuid4()}"


def _create_topic(name: str, partitions: int) -> str:
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for fut in client.create_topics(
        [NewTopic(name, partitions)], operation_timeout=5.0
    ).values():
        fut.result()
    return name


def _add_partitions(name: str, total: int) -> None:
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    for fut in client.create_partitions(
        [NewPartitions(name, total)], operation_timeout=10.0
    ).values():
        fut.result()


def _partition_count(topic: str) -> Optional[int]:
    """The topic's partition count, or `None` until every partition has a leader.

    Metadata can lag a successful create or add, and a new partition has no
    leader for a moment, so neither is an error here; `_await_partitions`
    retries.

    """
    client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    metadata = client.list_topics(topic, timeout=10.0).topics.get(topic)
    if metadata is None or metadata.error is not None:
        return None
    if any(part.leader < 0 for part in metadata.partitions.values()):
        return None
    return len(metadata.partitions)


def _readable(topic: str, count: int) -> bool:
    """Whether every partition answers a watermark read.

    A new partition can report a leader before the broker has initialised its
    log, and then answers `NOT_LEADER_FOR_PARTITION`, so that is not an error.

    """
    try:
        for partition in range(count):
            _high(topic, partition)
    except KafkaException as err:
        if err.args and err.args[0].code() == KafkaError.NOT_LEADER_FOR_PARTITION:
            return False
        raise
    return True


def _await_partitions(topic: str, count: int) -> None:
    """Precondition: wait until the topic reports `count` partitions, all readable."""
    deadline = time.monotonic() + 30.0
    while True:
        actual = _partition_count(topic)
        if actual == count and _readable(topic, count):
            return
        if time.monotonic() > deadline:
            err = f"`{topic}` reports {actual} partitions, expected {count}"
            raise AssertionError(err)
        time.sleep(0.2)


def _consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": f"eos-scale-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


def _high(topic: str, partition: int) -> int:
    consumer = _consumer()
    try:
        _low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=10.0
        )
        return high
    finally:
        consumer.close()


def _consume_all(topic: str) -> List[Record]:
    """Every message of every partition, each partition in offset order."""
    consumer = _consumer()
    try:
        count = _partition_count(topic)
        assert count is not None, f"metadata of `{topic}` is unavailable"
        assignments: List[TopicPartition] = []
        total = 0
        for partition in range(count):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=10.0
            )
            if high > low:
                assignments.append(TopicPartition(topic, partition, low))
                total += high - low
        consumer.assign(assignments)
        records: List[Record] = []
        deadline = time.monotonic() + 60.0
        while len(records) < total and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error() is not None:
                continue
            records.append((msg.partition(), msg.key(), msg.value()))
        assert len(records) == total, f"read {len(records)} of {total} messages"
        return records
    finally:
        consumer.close()


def _by_partition(records: List[Record]) -> Dict[int, List[Tuple[bytes, bytes]]]:
    """The `(key, value)` bytes of each partition, in offset order."""
    out: Dict[int, List[Tuple[bytes, bytes]]] = {}
    for partition, key, value in records:
        out.setdefault(partition, []).append((key, value))
    return out


def _expected(num_keys: int) -> List[Pair]:
    """The pipeline's results in input order, as `(key, "item:running_sum")`."""
    running: Dict[str, int] = {}
    out: List[Pair] = []
    for item in ITEMS:
        key = f"key{item % num_keys}"
        running[key] = running.get(key, 0) + item
        out.append((key, f"{item}:{running[key]}"))
    return out


def _assert_exactly_once(records: List[Record], expected: List[Pair]) -> None:
    """Every result once; each key in exactly one partition, in order."""
    decoded = [(key.decode(), value.decode()) for _p, key, value in records]
    assert Counter(decoded) == Counter(expected)
    for key in {k for k, _ in expected}:
        mine = [(p, v.decode()) for p, k, v in records if k.decode() == key]
        partitions = {p for p, _v in mine}
        assert len(partitions) == 1, f"key {key} spans partitions {sorted(partitions)}"
        assert [v for _p, v in mine] == [v for k, v in expected if k == key]


def _slow_running_sum(state, item):
    time.sleep(SLOW_SECONDS)
    total = (state or 0) + item
    return (total, (item, total))


def _encode(kv) -> KafkaSinkMessage:
    key, (item, total) = kv
    return KafkaSinkMessage(
        key=key.encode(), value=f"{item}:{total}".encode(), topic=None
    )


def _crash_on(value: int):
    """A map that raises the first time it sees `value`, a real crash."""
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


def _summed(stream, num_keys: int):
    """Per-key running sums, as `(key, (item, total))`."""
    keyed = op.map("key", stream, lambda item: (f"key{item % num_keys}", item))
    return op.stateful_map("sum", keyed, _slow_running_sum)


def _sink_items(stream, tag: str):
    """Items in the shape `KafkaEosSink` takes: `(routing key, KafkaSinkMessage)`.

    The routing key is the Kafka message key, so each key lands in the
    partition that its routing key's CRC-32 names.
    """
    msgs = op.map(f"encode{tag}", stream, _encode)
    return op.map(f"route{tag}", msgs, lambda msg: (msg.key.decode(), msg))


def _single_flow(topic: str, num_keys: int, on_item=None) -> Dataflow:
    flow = Dataflow("eos_scale")
    stream = op.input("inp", flow, TestingSource(ITEMS))
    if on_item is not None:
        stream = op.map("hook", stream, on_item)
    summed = _summed(stream, num_keys)
    op.output("out", _sink_items(summed, ""), KafkaEosSink([KAFKA_BROKER], topic))
    return flow


def _in_lane(lane: int, lanes: int) -> Callable[[Tuple[str, Tuple[int, int]]], bool]:
    def keep(kv: Tuple[str, Tuple[int, int]]) -> bool:
        item, _total = kv[1]
        return item % lanes == lane

    return keep


def _four_flow(topics: Sequence[str], on_item=None) -> Dataflow:
    """One input, split into one lane per topic, each with its own sink."""
    flow = Dataflow("eos_scale_four")
    stream = op.input("inp", flow, TestingSource(ITEMS))
    if on_item is not None:
        stream = op.map("hook", stream, on_item)
    summed = _summed(stream, FOUR_KEYS)
    for lane, topic in enumerate(topics):
        picked = op.filter(f"lane{lane}", summed, _in_lane(lane, len(topics)))
        op.output(
            f"out{lane}",
            _sink_items(picked, str(lane)),
            KafkaEosSink([KAFKA_BROKER], topic),
        )
    return flow


def _recovery_dir(path: Path, count: int) -> Path:
    """An initialised recovery directory with `count` partitions."""
    path.mkdir(parents=True, exist_ok=True)
    if not any(path.iterdir()):
        init_db_dir(path, count)
    return path


def _recovery(path: Path) -> RecoveryConfig:
    return RecoveryConfig(str(_recovery_dir(path, 1)), snapshot_interval=SNAPSHOT_TD)


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


def _names_fence(cause: str, topic: str, old: int, new: int) -> bool:
    """Whether `cause` is the partition-count refusal, naming both counts."""
    old_text = f"{old} partition{'' if old == 1 else 's'}"
    new_text = f"{new} partition{'' if new == 1 else 's'}"
    return f"`{topic}` with {old_text}, but `{topic}` has {new_text} now" in cause


def _run_expecting_crash(
    flow: Dataflow, recovery_config: RecoveryConfig, workers: int = 1
) -> None:
    """Run the execution that must crash mid-run, and check that it did."""
    try:
        _run(flow, recovery_config, workers)
    except Exception as err:
        cause = _root_cause(err)
        assert "simulated crash" in cause, f"unexpected failure: {cause}"
        return
    msg = "the execution finished, but it should have crashed mid-run"
    raise AssertionError(msg)


def _crc_partition(key: bytes, count: int) -> int:
    """The partition of `key` on a `count`-partition topic: its CRC-32, mod count."""
    return zlib.crc32(key) % count


def _assert_crc_placement(records: List[Record], count: int) -> None:
    """Every record sits in the partition its key's CRC-32 names."""
    for partition, key, _value in records:
        assert partition == _crc_partition(key, count), (key, partition)


def _free_address() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def _spawn(tmp_path: Path, tag: str, recovery_dir: Path, topic: str) -> List[Child]:
    """Start one process per address of a fresh two-process cluster."""
    addresses = [_free_address() for _ in range(2)]
    children: List[Child] = []
    for proc_id in range(len(addresses)):
        log = tmp_path / f"{tag}-{proc_id}.log"
        with log.open("w", encoding="utf-8") as out:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    str(recovery_dir),
                    topic,
                    str(proc_id),
                    *addresses,
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        children.append((proc, log))
    return children


def _logs(children: List[Child]) -> str:
    return "".join(log.read_text(encoding="utf-8") for _proc, log in children)


def _kill(children: List[Child]) -> None:
    for proc, _log in children:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=30)


def _await_gate(children: List[Child], topic: str, held: Optional[Path] = None) -> None:
    """Wait until the topic holds `GATE_MESSAGES`, and `held` exists, if given.

    Fails early if a process exits, since the gate can then never open.
    """
    deadline = time.monotonic() + GATE_TIMEOUT_S
    while True:
        exited = [proc.poll() for proc, _log in children]
        if any(code is not None for code in exited):
            text = _logs(children)
            _kill(children)
            msg = f"a process exited before the gate:\n{text}"
            raise AssertionError(msg)
        if _high(topic, 0) >= GATE_MESSAGES and (held is None or held.exists()):
            return
        if time.monotonic() > deadline:
            _kill(children)
            msg = f"the gate did not open within {GATE_TIMEOUT_S} s"
            raise AssertionError(msg)
        time.sleep(0.1)


def _crash_one(children: List[Child]) -> None:
    """SIGKILL the second process; the first is left to fail or to be killed."""
    victim, _log = children[1]
    victim.kill()
    victim.wait(timeout=30)
    try:
        children[0][0].wait(timeout=60)
    except subprocess.TimeoutExpired:
        pass


def _execute(tmp_path: Path, tag: str, recovery_dir: Path, topic: str) -> None:
    """Run both processes to completion; each must exit cleanly."""
    children = _spawn(tmp_path, tag, recovery_dir, topic)
    try:
        for proc, log in children:
            code = proc.wait(timeout=300)
            text = log.read_text(encoding="utf-8")
            assert code == 0, f"{tag}: exit code {code}\n{text}"
    finally:
        _kill(children)


def _holding(directory: Path):
    """A map that holds its worker at item `HOLD_AT` until the test releases it.

    The worker writes `held` in `directory` when it reaches the item, and then
    waits for `release` there. Each wait has a hang guard.
    """

    def hold(item):
        if item == HOLD_AT:
            (directory / "held").write_text("held", encoding="utf-8")
            deadline = time.monotonic() + GATE_TIMEOUT_S
            while not (directory / "release").exists():
                if time.monotonic() > deadline:
                    msg = f"the test never released the hold at {HOLD_AT}"
                    raise RuntimeError(msg)
                time.sleep(0.01)
        return item

    return hold


def _child_main(argv: List[str]) -> None:
    """One process of a cluster: `RECOVERY_DIR TOPIC PROC_ID [ADDRESS...]`.

    With no addresses the process runs alone, as a single-process execution.
    `EOS_CHILD_KEYS` sets the number of keys, which defaults to `NUM_KEYS`.
    `EOS_CHILD_HOLD` names a directory in which the process holds at
    `HOLD_AT`, see `_holding`.
    """
    recovery_dir, topic, proc_id, *addresses = argv
    keys = int(os.environ.get("EOS_CHILD_KEYS", NUM_KEYS))
    hold = os.environ.get("EOS_CHILD_HOLD")
    on_item = _holding(Path(hold)) if hold else None
    cluster_main(
        _single_flow(topic, keys, on_item=on_item),
        addresses,
        int(proc_id),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(recovery_dir, snapshot_interval=SNAPSHOT_TD),
    )


def test_eos_multi_partition_topic_exactly_once(tmp_path):
    """A 50-partition topic, a keyed workload, a crash and two resumes.

    Every result appears once in the topic. Each key's results lie in one
    partition, the one its CRC-32 names, and the bytes of every partition
    equal those of an uninterrupted run into a second 50-partition topic. So
    each key sits in the same partition across executions, and replay
    produces the same bytes.

    """
    topic = _create_topic(_name("wide"), WIDE_PARTITIONS)
    clean_topic = _create_topic(_name("wide-clean"), WIDE_PARTITIONS)
    _await_partitions(topic, WIDE_PARTITIONS)
    _await_partitions(clean_topic, WIDE_PARTITIONS)

    crashed = _recovery(tmp_path / "crashed")
    _run_expecting_crash(
        _single_flow(topic, WIDE_KEYS, on_item=_crash_on(MID_WINDOW)), crashed
    )
    _run(_single_flow(topic, WIDE_KEYS), crashed)
    _run(_single_flow(topic, WIDE_KEYS), crashed)

    clean = _recovery(tmp_path / "clean")
    _run(_single_flow(clean_topic, WIDE_KEYS), clean)
    _run(_single_flow(clean_topic, WIDE_KEYS), clean)

    records = _consume_all(topic)
    _assert_exactly_once(records, _expected(WIDE_KEYS))
    _assert_crc_placement(records, WIDE_PARTITIONS)
    assert _by_partition(records) == _by_partition(_consume_all(clean_topic))


def test_eos_four_topics_twenty_partitions(tmp_path):
    """Four 20-partition topics, each fed by its own `op.output` step.

    The input is split into four lanes by item, so each key feeds one topic.
    Each topic must satisfy the same assertions as the 50-partition test,
    checked against an uninterrupted run into four reference topics.

    """
    topics = [_create_topic(_name(f"four-{i}"), FOUR_PARTITIONS) for i in range(4)]
    clean_topics = [
        _create_topic(_name(f"four-clean-{i}"), FOUR_PARTITIONS) for i in range(4)
    ]
    for topic in topics + clean_topics:
        _await_partitions(topic, FOUR_PARTITIONS)

    crashed = _recovery(tmp_path / "crashed")
    _run_expecting_crash(_four_flow(topics, on_item=_crash_on(MID_WINDOW)), crashed)
    _run(_four_flow(topics), crashed)
    _run(_four_flow(topics), crashed)

    clean = _recovery(tmp_path / "clean")
    _run(_four_flow(clean_topics), clean)
    _run(_four_flow(clean_topics), clean)

    expected = _expected(FOUR_KEYS)
    for lane, (topic, clean_topic) in enumerate(zip(topics, clean_topics)):
        lane_expected = [
            (key, value)
            for key, value in expected
            if int(value.split(":")[0]) % FOUR_TOPICS == lane
        ]
        records = _consume_all(topic)
        _assert_exactly_once(records, lane_expected)
        _assert_crc_placement(records, FOUR_PARTITIONS)
        assert _by_partition(records) == _by_partition(_consume_all(clean_topic))


def test_eos_multi_process_exactly_once(tmp_path):
    """A two-process cluster writes one single-partition topic.

    The second process is SIGKILLed once the topic holds some results. Both
    processes are then resumed, and a finishing execution follows. The topic
    must hold every result exactly once.

    """
    topic = _create_topic(_name("procs"), 1)
    _await_partitions(topic, 1)
    recovery_dir = _recovery_dir(tmp_path / "recovery", 2)

    children = _spawn(tmp_path, "first", recovery_dir, topic)
    try:
        _await_gate(children, topic)
        _crash_one(children)
    finally:
        _kill(children)
    _execute(tmp_path, "resume", recovery_dir, topic)
    _execute(tmp_path, "finish", recovery_dir, topic)

    _assert_exactly_once(_consume_all(topic), _expected(NUM_KEYS))


def test_eos_process_without_a_recovery_lock_leaves_its_windows_for_the_next_execution(
    tmp_path, monkeypatch
):
    """With one recovery partition, the process with no lock publishes nothing at close.

    Two processes share one recovery partition, so only one of them holds its
    lock. When the run completes, the other could still be publishing while
    a successor starts, so it leaves its last window in the state and warns.
    The topic lacks that window until the next execution, which then publishes
    every result exactly once.

    """
    # Eight keys hash onto all four partitions, and the processes split those
    # partitions between them. Four keys hash onto two, and one process would
    # own them all.
    keys = 8
    monkeypatch.setenv("EOS_CHILD_KEYS", str(keys))
    topic = _create_topic(_name("lockless"), 4)
    _await_partitions(topic, 4)
    recovery_dir = _recovery_dir(tmp_path / "recovery", 1)

    children = _spawn(tmp_path, "first", recovery_dir, topic)
    try:
        for proc, log in children:
            code = proc.wait(timeout=300)
            assert code == 0, f"exit code {code}\n{log.read_text(encoding='utf-8')}"
    finally:
        _kill(children)
    warned = [
        log
        for _proc, log in children
        if "at least as many recovery partitions" in log.read_text(encoding="utf-8")
    ]
    assert len(warned) == 1, f"{len(warned)} processes warned, expected one"

    expected = _expected(keys)
    published = Counter(
        (key.decode(), value.decode()) for _p, key, value in _consume_all(topic)
    )
    assert published != Counter(expected), "the lockless process published at close"

    monkeypatch.delenv("EOS_CHILD_KEYS")
    _run(_single_flow(topic, keys), _recovery(recovery_dir))
    _assert_exactly_once(_consume_all(topic), expected)


def _spawn_alone(
    tmp_path: Path,
    tag: str,
    recovery_dir: Path,
    topic: str,
    hold: Optional[Path] = None,
) -> Child:
    """Start one process that runs the flow alone, as a single-process execution.

    With `hold`, the process holds at `HOLD_AT` until `hold/release` exists.
    """
    log = tmp_path / f"{tag}.log"
    env = {**os.environ, "EOS_CHILD_HOLD": str(hold)} if hold is not None else None
    with log.open("w", encoding="utf-8") as out:
        proc = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                str(recovery_dir),
                topic,
                "0",
            ],
            stdout=out,
            stderr=subprocess.STDOUT,
            env=env,
        )
    return proc, log


@mark.skipif(
    sys.platform == "win32",
    reason="stops the predecessor with SIGSTOP and SIGCONT, which Windows lacks",
)
def test_eos_successor_refused_while_predecessor_is_frozen(tmp_path):
    """A frozen predecessor keeps the recovery lock, so its successor is refused.

    The predecessor runs in its own process over a recovery store, and holds
    mid-run at `HOLD_AT`, with its lock taken and results in the topic. The
    test stops it with SIGSTOP while it holds, and a successor on the same
    store is refused, and names the lock. Then the predecessor is resumed,
    released, and finishes alone: no finishing execution runs, yet the topic
    holds every result exactly once.

    """
    topic = _create_topic(_name("frozen"), 1)
    _await_partitions(topic, 1)
    recovery_dir = _recovery_dir(tmp_path / "recovery", 1)
    hold = tmp_path / "hold"
    hold.mkdir()

    children = [_spawn_alone(tmp_path, "predecessor", recovery_dir, topic, hold)]
    victim, log = children[0]
    try:
        _await_gate(children, topic, held=hold / "held")
        assert victim.poll() is None, log.read_text(encoding="utf-8")
        os.kill(victim.pid, signal.SIGSTOP)
        try:
            with raises(RuntimeError) as excinfo:
                _run(_single_flow(topic, NUM_KEYS), _recovery(recovery_dir))
            assert "in use by another execution" in _root_cause(excinfo.value)
        finally:
            os.kill(victim.pid, signal.SIGCONT)
            (hold / "release").write_text("release", encoding="utf-8")
        code = victim.wait(timeout=300)
        assert code == 0, log.read_text(encoding="utf-8")
    finally:
        _kill(children)

    _assert_exactly_once(_consume_all(topic), _expected(NUM_KEYS))


def test_eos_partition_count_change_is_refused(tmp_path):
    """A topic that gains partitions between executions is refused, deterministically.

    The first execution runs on one partition. Then the topic grows to four,
    and a second execution on the same store is refused, twice, with no
    output written. The refusal names both counts.

    """
    topic = _create_topic(_name("grow"), 1)
    _await_partitions(topic, 1)
    recovery_config = _recovery(tmp_path)
    flow = _single_flow(topic, NUM_KEYS)
    _run(flow, recovery_config)
    published = _high(topic, 0)

    _add_partitions(topic, GROWN_PARTITIONS)
    _await_partitions(topic, GROWN_PARTITIONS)

    causes: List[str] = []
    for _attempt in range(2):
        try:
            _run(flow, recovery_config)
        except Exception as err:
            causes.append(_root_cause(err))
        else:
            msg = "an execution ran on a topic that gained partitions"
            raise AssertionError(msg)
    for cause in causes:
        assert _names_fence(cause, topic, 1, GROWN_PARTITIONS), cause
    assert _high(topic, 0) == published, "a refused execution wrote partition 0"
    for partition in range(1, GROWN_PARTITIONS):
        assert _high(topic, partition) == 0, f"a refused execution wrote {partition}"


def test_eos_partitions_spread_over_workers_stay_exactly_once(tmp_path, monkeypatch):
    """A 50-partition topic on four workers: partitions run on several threads.

    Each build records the thread that built its partition. The engine builds
    a partition when items first reach it, so only the partitions that hold
    keys are built. A crash can stop a run before it has built many of them,
    so the builds are counted over the whole test, once its completed runs
    have reached every key. Those must be built on more than one thread, and
    the output must satisfy the same assertions as the one-worker test.

    """
    topic = _create_topic(_name("workers"), WIDE_PARTITIONS)
    clean_topic = _create_topic(_name("workers-clean"), WIDE_PARTITIONS)
    _await_partitions(topic, WIDE_PARTITIONS)
    _await_partitions(clean_topic, WIDE_PARTITIONS)

    builds: List[Tuple[int, str]] = []
    real_build = KafkaEosSink.build_part

    def recording_build(self, step_id, for_part, resume_state):
        builds.append((threading.get_ident(), for_part))
        return real_build(self, step_id, for_part, resume_state)

    monkeypatch.setattr(KafkaEosSink, "build_part", recording_build)
    crashed = _recovery(tmp_path / "crashed")
    _run_expecting_crash(
        _single_flow(topic, WIDE_KEYS, on_item=_crash_on(MID_WINDOW)),
        crashed,
        workers=4,
    )
    _run(_single_flow(topic, WIDE_KEYS), crashed, workers=4)
    _run(_single_flow(topic, WIDE_KEYS), crashed, workers=4)

    clean = _recovery(tmp_path / "clean")
    _run(_single_flow(clean_topic, WIDE_KEYS), clean, workers=4)
    _run(_single_flow(clean_topic, WIDE_KEYS), clean, workers=4)

    parts = {part for _thread, part in builds}
    assert len(parts) >= WIDE_KEYS // 2, f"only {len(parts)} partitions were built"
    threads = {thread for thread, _part in builds}
    assert len(threads) > 1, f"the partitions were built on {len(threads)} thread"

    records = _consume_all(topic)
    _assert_exactly_once(records, _expected(WIDE_KEYS))
    _assert_crc_placement(records, WIDE_PARTITIONS)
    assert _by_partition(records) == _by_partition(_consume_all(clean_topic))


def test_eos_delivery_failure_on_one_partition_then_resume_completes(
    tmp_path, monkeypatch
):
    """One partition's delivery failure fails the run; the resume publishes each once.

    Eight partitions share one worker's clients, so they share one producer.
    The first delivery report of the partition that holds `key0` is turned
    into a failure. That message did land, so the failure is a lost
    acknowledgement, and the resume must not publish it twice. The run must
    fail with the delivery error, and the two resumes must give every
    partition exactly once.

    """
    partitions = 8
    topic = _create_topic(_name("fail"), partitions)
    clean_topic = _create_topic(_name("fail-clean"), partitions)
    _await_partitions(topic, partitions)
    _await_partitions(clean_topic, partitions)
    failing = _crc_partition(b"key0", partitions)

    injected = {"done": False}
    real_on_delivery = eos._EosPartition._on_delivery

    def lose_one_ack(self, window, err, msg):
        if err is None and self._partition == failing and not injected["done"]:
            injected["done"] = True
            real_on_delivery(self, window, "injected: acknowledgement lost", msg)
            return
        real_on_delivery(self, window, err, msg)

    monkeypatch.setattr(eos._EosPartition, "_on_delivery", lose_one_ack)
    recovery = _recovery(tmp_path / "failed")
    flow = _single_flow(topic, WIDE_KEYS)
    with raises(RuntimeError) as excinfo:
        _run(flow, recovery)
    assert injected["done"], "the delivery failure was never injected"
    assert "could not deliver" in _root_cause(excinfo.value)

    _run(flow, recovery)
    _run(flow, recovery)
    clean = _recovery(tmp_path / "clean")
    _run(_single_flow(clean_topic, WIDE_KEYS), clean)
    _run(_single_flow(clean_topic, WIDE_KEYS), clean)

    records = _consume_all(topic)
    _assert_exactly_once(records, _expected(WIDE_KEYS))
    assert _by_partition(records) == _by_partition(_consume_all(clean_topic))


if __name__ == "__main__":
    # The processes of `test_eos_multi_process_exactly_once`.
    _child_main(sys.argv[1:])
