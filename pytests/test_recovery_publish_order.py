"""Publication order: a snapshot finds the one before it durable.

`KafkaEosSink` publishes the window that a snapshot closed at the next
snapshot. That is safe only because the engine takes a sink partition's
snapshot for a later boundary only after every recovery write for the
earlier boundaries is durable, in every process. The tests here check
that with a sink partition that reads the recovery store at each of its
own snapshots, and check what `close()` can see of the execution's
outcome. They need no broker.

The multi-process test runs this file as a script, one process of the
cluster per run. See `_child_main`.

"""

import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import bytewax.operators as op
from bytewax._bytewax import execution_state
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from bytewax.tracing import setup_tracing
from pytest import mark, raises

# Recovery stores use STRICT tables, which need SQLite 3.37. Python 3.8 on
# Windows bundles an older SQLite, so tests that read a store with
# `sqlite3` skip there.
requires_strict_tables = mark.skipif(
    sqlite3.sqlite_version_info < (3, 37, 0),
    reason=f"sqlite3 {sqlite3.sqlite_version} predates STRICT tables (3.37)",
)


EPOCH_TD = timedelta(seconds=0.02)
SNAPSHOT_TD = timedelta(seconds=0.06)
PARTS = ["p0", "p1", "p2", "p3"]
ITEMS = list(range(120))
# Checks that must run for a test to mean anything.
MIN_CHECKS = 10

# The recovery directory the partitions in this process read.
_recovery_dir: Optional[Path] = None
# (state key, snapshot number) of every check this process made.
_checks: List[Tuple[str, int]] = []
# The execution state each partition saw when it closed.
_closed_with: List[Tuple[bool, bool]] = []


_READER = """
import glob
import os
import sqlite3
import sys

d, step_id, key = sys.argv[1:4]
best = None
for path in glob.glob(os.path.join(d, "part-*.sqlite3")):
    conn = sqlite3.connect(path, timeout=10)
    row = conn.execute(
        "SELECT snap_epoch, ser_change FROM snaps "
        "WHERE step_id = ? AND state_key = ? ORDER BY snap_epoch DESC LIMIT 1",
        (step_id, key),
    ).fetchone()
    conn.close()
    if row is not None and (best is None or row[0] > best[0]):
        best = (row[0], bytes(row[1]))
sys.stdout.buffer.write(b"" if best is None else best[1])
"""


# Read out of process: Python's sqlite3 and the engine's SQLite are two copies in
# one process, and closing the reader can delete the WAL the engine writes into.
def _latest_snapshot(step_id: str, state_key: str) -> Optional[bytes]:
    """Read in a separate process: two SQLite copies in one process break WAL."""
    assert _recovery_dir is not None
    out = subprocess.run(
        [sys.executable, "-I", "-c", _READER, str(_recovery_dir), step_id, state_key],
        capture_output=True,
        check=True,
    ).stdout
    return out or None


class _CheckingPartition(StatefulSinkPartition):
    """Checks that the previous snapshot of this partition is in the store.

    Each snapshot's state carries a fresh marker, which pickle stores as
    raw bytes. At the next snapshot the newest stored row must contain the
    previous marker, so the previous snapshot is durable.

    """

    def __init__(self, step_id: str, key: str, resume_state):
        self._step_id = step_id
        self._key = key
        self._count = 0
        self._marker: Optional[bytes] = None
        if resume_state is not None:
            self._count, self._marker = resume_state

    def write_batch(self, values):
        pass

    def snapshot(self):
        if self._marker is not None:
            stored = _latest_snapshot(self._step_id, self._key)
            if stored is None or self._marker not in stored:
                msg = (
                    f"snapshot {self._count} of `{self._key}` is not in the "
                    "recovery store when the next snapshot runs"
                )
                raise RuntimeError(msg)
            _checks.append((self._key, self._count))
        self._count += 1
        self._marker = f"marker-{uuid.uuid4().hex}-{self._count}".encode()
        return (self._count, self._marker)

    def close(self):
        _closed_with.append(execution_state())


class _CheckingSink(FixedPartitionedSink):
    def list_parts(self):
        return PARTS

    def part_fn(self, item_key: str) -> int:
        return int(item_key[1:])

    def build_part(self, step_id, for_part, resume_state):
        return _CheckingPartition(step_id, for_part, resume_state)


class _RecordingPartition(StatefulSinkPartition):
    _close_after_run = True

    """Records what `execution_state()` says when it closes, and nothing else."""

    def write_batch(self, values):
        pass

    def snapshot(self):
        return None

    def close(self):
        _closed_with.append(execution_state())


class _RecordingSink(FixedPartitionedSink):
    def list_parts(self):
        return PARTS

    def part_fn(self, item_key: str) -> int:
        return int(item_key[1:])

    def build_part(self, step_id, for_part, resume_state):
        return _RecordingPartition()


def _slow(item):
    time.sleep(0.02)
    return item


def _flow(crash_at: Optional[int] = None, sink=None) -> Dataflow:
    def maybe_crash(item):
        if crash_at is not None and item == crash_at:
            msg = f"simulated crash on {crash_at}"
            raise RuntimeError(msg)
        return item

    flow = Dataflow("publish_order")
    s = op.input("inp", flow, TestingSource(ITEMS))
    s = op.map("slow", s, _slow)
    s = op.map("crash", s, maybe_crash)
    keyed = op.map("key", s, lambda i: (f"p{i % 4}", i))
    op.output("out", keyed, _CheckingSink() if sink is None else sink)
    return flow


def _recovery(path: Path, count: int) -> RecoveryConfig:
    path.mkdir(parents=True, exist_ok=True)
    init_db_dir(path, count)
    return RecoveryConfig(str(path), snapshot_interval=SNAPSHOT_TD)


def _reset(path: Path) -> None:
    global _recovery_dir  # noqa: PLW0603
    _recovery_dir = path
    _checks.clear()
    _closed_with.clear()


@requires_strict_tables
def test_each_snapshot_finds_the_one_before_it_durable(tmp_path):
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    run_main(_flow(), epoch_interval=EPOCH_TD, recovery_config=recovery)
    assert len(_checks) >= MIN_CHECKS, f"only {len(_checks)} checks ran"


@requires_strict_tables
def test_each_snapshot_finds_the_one_before_it_durable_on_two_workers(tmp_path):
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 2)
    cluster_main(
        _flow(),
        [],
        0,
        epoch_interval=EPOCH_TD,
        recovery_config=recovery,
        worker_count_per_proc=2,
    )
    assert len(_checks) >= MIN_CHECKS, f"only {len(_checks)} checks ran"


def _free_address() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


@requires_strict_tables
def test_each_snapshot_finds_the_one_before_it_durable_across_processes(tmp_path):
    """Two processes share one store; each one's snapshots are checked.

    A partition's snapshot is written by the process that owns its recovery
    partition, which need not be the process that holds the sink partition.
    So the check spans processes, and each child reports how many it made.

    """
    recovery_dir = tmp_path / "rec"
    _recovery(recovery_dir, 2)
    addresses = [_free_address() for _ in range(2)]
    children = []
    for proc_id in range(len(addresses)):
        log = tmp_path / f"proc-{proc_id}.log"
        with log.open("w", encoding="utf-8") as out:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    str(recovery_dir),
                    str(proc_id),
                    *addresses,
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        children.append((proc, log))

    checked = 0
    for proc, log in children:
        code = proc.wait(timeout=300)
        text = log.read_text(encoding="utf-8")
        assert code == 0, f"exit code {code}\n{text}"
        checked += sum(
            int(line.split()[1])
            for line in text.splitlines()
            if line.startswith("CHECKED ")
        )
    assert checked >= MIN_CHECKS, f"only {checked} checks ran"


def test_close_sees_a_completed_execution_with_recovery(tmp_path):
    """Partitions close after the run completes, with its outcome known.

    The sink partitions see the end of input, and close with the run's
    outcome, not before it. A close during the run would see `completed`
    false.

    """
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    run_main(
        _flow(sink=_RecordingSink()), epoch_interval=EPOCH_TD, recovery_config=recovery
    )
    assert _closed_with, "no partition closed"
    assert set(_closed_with) == {(True, True, True)}, _closed_with


def test_close_sees_no_completion_after_a_crash(tmp_path):
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    with raises(RuntimeError) as excinfo:
        run_main(
            _flow(crash_at=60, sink=_RecordingSink()),
            epoch_interval=EPOCH_TD,
            recovery_config=recovery,
        )
    cause = excinfo.value
    while cause.__cause__ is not None:
        cause = cause.__cause__
    assert "simulated crash" in str(cause)
    assert _closed_with, "no partition closed"
    assert all(
        not completed for _recovering, completed, _held in _closed_with
    ), _closed_with


def test_close_without_recovery_runs_before_the_run_ends(tmp_path):
    """Without recovery a partition closes as it is dropped, during the run.

    The run has no recovery config, so nothing is deferred. `close` sees the
    run's outcome as not yet recorded: a deferred close would see `completed`
    true, since this run completes.

    """
    _reset(tmp_path)
    run_main(_flow(sink=_RecordingSink()), epoch_interval=EPOCH_TD)
    assert _closed_with, "no partition closed"
    assert set(_closed_with) == {(False, False, False)}, _closed_with


class _HoldingClosePartition(StatefulSinkPartition):
    """Holds its worker in `close()` after a completed run, for the partition `p1`.

    `closing` is written when the hold starts, and `done` when it ends. The
    hold ends when the test writes `release`, so the test decides how long the
    close runs, and no timing is involved.

    """

    _close_after_run = True

    def __init__(self, key: str, closing: Path, release: Path, done: Path):
        self._key = key
        self._closing = closing
        self._release = release
        self._done = done

    def write_batch(self, values):
        pass

    def snapshot(self):
        return None

    def close(self):
        if self._key == "p1" and execution_state()[1]:
            self._closing.write_text("closing", encoding="utf-8")
            _hold_until(self._release, "the close of `p1` was never released")
            self._done.write_text("done", encoding="utf-8")


class _HoldingCloseSink(FixedPartitionedSink):
    def __init__(self, closing: Path, release: Path, done: Path):
        self._closing = closing
        self._release = release
        self._done = done

    def list_parts(self):
        return PARTS

    def part_fn(self, item_key: str) -> int:
        return int(item_key[1:])

    def build_part(self, step_id, for_part, resume_state):
        return _HoldingClosePartition(
            for_part, self._closing, self._release, self._done
        )


# Written by worker 0 once it has shut down, which parks its recovery lock.
# The tests wait for it before they start a successor.
_WORKER_STOP = "Worker stop worker=0"
# An upper bound on each wait for a marker. It only guards against a hang: a
# wait ends as soon as its marker appears, and a timeout names what was missing.
MARK_TIMEOUT_S = 60.0


def _hold_until(path: Path, what: str) -> None:
    """Child side: block until the test writes `path`, with a hang guard."""
    deadline = time.monotonic() + MARK_TIMEOUT_S
    while not path.exists():
        if time.monotonic() > deadline:
            msg = f"{what}: no `{path.name}` within {MARK_TIMEOUT_S} s"
            raise RuntimeError(msg)
        time.sleep(0.01)


def _predecessor_main(recovery_dir: str, closing: str, release: str, done: str) -> None:
    """Run the predecessor: two workers, recovery on worker 0, `p1` on worker 1.

    Placement is what the engine's primary assignment gives for one recovery
    partition and two sink partitions on two workers: recovery partition 0
    and `p0` on worker 0, and `p1` on worker 1. Worker 1 holds in the close
    of `p1` until the test writes `release`.

    """
    flow = Dataflow("predecessor")
    s = op.input("inp", flow, TestingSource(list(range(8))))
    keyed = op.map("key", s, lambda i: (f"p{i % 2}", i))
    sink = _HoldingCloseSink(Path(closing), Path(release), Path(done))
    op.output("out", keyed, sink)
    cluster_main(
        flow,
        [],
        0,
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(recovery_dir, snapshot_interval=SNAPSHOT_TD),
        worker_count_per_proc=2,
    )


def _log_text(log: Path) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", log.read_text(encoding="utf-8"))


def _await_child(
    child: subprocess.Popen, log: Path, ready: Callable[[str], bool], what: str
) -> None:
    """Parent side: wait until `ready` holds for the child's log.

    Fails with the log if the child exits first, since `what` can then never
    happen, and if the wait outlasts `MARK_TIMEOUT_S`.

    """
    deadline = time.monotonic() + MARK_TIMEOUT_S
    while not ready(_log_text(log)):
        if child.poll() is not None:
            msg = f"the child exited before {what}:\n{_log_text(log)}"
            raise AssertionError(msg)
        if time.monotonic() > deadline:
            msg = f"{what} did not happen within {MARK_TIMEOUT_S} s:\n{_log_text(log)}"
            raise AssertionError(msg)
        time.sleep(0.02)


def test_successor_is_refused_while_a_sibling_worker_still_closes(tmp_path):
    """A successor is refused while a worker that holds no recovery lock closes.

    The predecessor runs in its own process with two workers. Worker 0 holds
    the store's only recovery partition and shuts down first. Worker 1 holds
    no recovery partition, and it is held in the completed `close()` of its
    sink partition until the test releases it. A successor on the same store
    must be refused while that close is held, even though worker 0 has
    already shut down. The test waits for worker 0's shutdown through its log
    line, and for the close through its marker file, before it starts the
    successor.

    """
    recovery_dir = tmp_path / "rec"
    recovery_dir.mkdir()
    init_db_dir(recovery_dir, 1)
    closing = tmp_path / "closing"
    release = tmp_path / "release"
    done = tmp_path / "done"
    log = tmp_path / "predecessor.log"
    with log.open("w", encoding="utf-8") as out:
        child = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "predecessor",
                str(recovery_dir),
                str(closing),
                str(release),
                str(done),
            ],
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    try:
        _await_child(
            child,
            log,
            lambda text: closing.exists() and _WORKER_STOP in text,
            "worker 0 stopped while worker 1 was in its close",
        )
        assert not done.exists(), "the sibling's close had already finished"

        with raises(RuntimeError) as excinfo:
            run_main(
                _flow(sink=_RecordingSink()),
                epoch_interval=EPOCH_TD,
                recovery_config=RecoveryConfig(str(recovery_dir)),
            )
        cause = excinfo.value
        while cause.__cause__ is not None:
            cause = cause.__cause__
        assert "in use by another execution" in str(cause), str(cause)
        assert (
            child.poll() is None and not done.exists()
        ), "the successor was refused only after the sibling's close finished"

        release.write_text("release", encoding="utf-8")
        code = child.wait(timeout=MARK_TIMEOUT_S)
        assert code == 0, _log_text(log)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)


class _FailingClosePartition(StatefulSinkPartition):
    """Opts in to the deferred close, and fails in it, after recording itself."""

    _close_after_run = True

    def __init__(self, key: str, closed: List[str]):
        self._key = key
        self._closed = closed

    def write_batch(self, values):
        pass

    def snapshot(self):
        return None

    def close(self):
        self._closed.append(self._key)
        msg = f"injected close failure in {self._key}"
        raise RuntimeError(msg)


class _FailingCloseSink(FixedPartitionedSink):
    def __init__(self, closed: List[str]):
        self._closed = closed

    def list_parts(self):
        return PARTS

    def part_fn(self, item_key: str) -> int:
        return int(item_key[1:])

    def build_part(self, step_id, for_part, resume_state):
        return _FailingClosePartition(for_part, self._closed)


def test_release_closes_every_partition_when_one_close_fails(tmp_path):
    """A failing close does not keep the other partitions of the run open.

    Every opted-in partition of the worker is closed when the run ends, even
    after one of them has failed, and the run reports that failure.

    """
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    closed: List[str] = []
    with raises(RuntimeError) as excinfo:
        run_main(
            _flow(sink=_FailingCloseSink(closed)),
            epoch_interval=EPOCH_TD,
            recovery_config=recovery,
        )
    cause = excinfo.value
    while cause.__cause__ is not None:
        cause = cause.__cause__
    assert "injected close failure" in str(cause), str(cause)
    assert sorted(closed) == PARTS, closed


class _PlainPartition(StatefulSinkPartition):
    """Does not opt in, so it closes as upstream does: when it is dropped."""

    def __init__(self, closes: List[float]):
        self._closes = closes

    def write_batch(self, values):
        pass

    def snapshot(self):
        return None

    def close(self):
        self._closes.append(time.monotonic())


class _PlainSink(FixedPartitionedSink):
    def __init__(self, closes: List[float]):
        self._closes = closes

    def list_parts(self):
        return PARTS

    def part_fn(self, item_key: str) -> int:
        return int(item_key[1:])

    def build_part(self, step_id, for_part, resume_state):
        return _PlainPartition(self._closes)


def _branches_flow(sink, marks: List[float], out: list, slow_input) -> Dataflow:
    """A sink fed by a short input, and a slow input that outlives it.

    `marks` records the time of each item of the slow input, so its last
    entry is when the slow branch finished.

    """
    flow = Dataflow("branches")
    fast = op.input("fast", flow, TestingSource(list(range(4))))
    keyed = op.map("key", fast, lambda i: (f"p{i % 4}", i))
    op.output("out", keyed, sink)
    slow = op.input("slow_in", flow, slow_input)
    slow = op.map("slow", slow, _slow)

    def mark(item):
        marks.append(time.monotonic())
        return item

    slow = op.map("mark", slow, mark)
    op.output("slow_out", slow, TestingSink(out))
    return flow


class _HeldSource(DynamicSource):
    """A slow input whose last item waits until a partition has closed."""

    def __init__(self, count: int, closes: List[float], timed_out: List[bool]):
        self._count = count
        self._closes = closes
        self._timed_out = timed_out

    def build(self, step_id, worker_index, worker_count):
        return _HeldPartition(list(range(self._count)), self._closes, self._timed_out)


class _HeldPartition(StatelessSourcePartition):
    """Yields its items without blocking, and holds the last one until a close.

    Holding is cooperative: `next_batch` returns nothing until `closes` has an
    entry, so the worker stays free to close the partitions. A hold that
    outlasts `MARK_TIMEOUT_S` ends anyway, and sets `timed_out`, so the test
    can report that nothing closed during the run.

    """

    def __init__(self, items: List[int], closes: List[float], timed_out: List[bool]):
        self._items = items
        self._closes = closes
        self._timed_out = timed_out
        self._deadline = time.monotonic() + MARK_TIMEOUT_S

    def next_batch(self):
        if not self._items:
            raise StopIteration
        if len(self._items) == 1 and not self._released():
            return []
        return [self._items.pop(0)]

    def next_awake(self):
        return datetime.now(timezone.utc) + timedelta(milliseconds=5)

    def _released(self) -> bool:
        if self._closes:
            return True
        if time.monotonic() > self._deadline:
            self._timed_out.append(True)
            return True
        return False


def test_close_of_a_partition_without_the_opt_in_runs_as_upstream(tmp_path):
    """A partition that does not opt in closes when it is dropped, not at the end.

    The sink's input ends early, and the slow input keeps the run going. Upstream
    drops the partitions at that end of input, so they close during the run.
    The slow input's last item is held until the first close, so the last mark
    is after a close by construction, and the run cannot pass for a deferred
    close by timing alone.

    """
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    closes: List[float] = []
    marks: List[float] = []
    timed_out: List[bool] = []
    run_main(
        _branches_flow(
            _PlainSink(closes),
            marks,
            [],
            _HeldSource(30, closes, timed_out),
        ),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery,
    )
    assert not timed_out, "no partition closed while the run was going"
    assert len(closes) == len(PARTS), closes
    assert min(closes) < marks[-1], "the partitions closed only after the run"


def test_close_of_a_partition_with_the_opt_in_runs_after_the_run(tmp_path):
    _reset(tmp_path / "rec")
    recovery = _recovery(tmp_path / "rec", 1)
    closes: List[float] = []
    marks: List[float] = []
    run_main(
        _branches_flow(_OptInSink(closes), marks, [], TestingSource(list(range(30)))),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery,
    )
    assert len(closes) == len(PARTS), closes
    assert min(closes) > marks[-1], "a deferred close ran during the run"


class _OptInPartition(_PlainPartition):
    """Opts in to the deferred close, and otherwise behaves as `_PlainPartition`."""

    _close_after_run = True


class _OptInSink(_PlainSink):
    def build_part(self, step_id, for_part, resume_state):
        return _OptInPartition(self._closes)


def _unwind_main(recovery_dir: str) -> None:
    """A run that crashes while its opted-in partitions fail to close."""
    closed: List[str] = []
    run_main(
        _flow(crash_at=60, sink=_FailingCloseSink(closed)),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(recovery_dir, snapshot_interval=SNAPSHOT_TD),
    )


def test_a_failing_close_while_a_crash_unwinds_is_logged_not_aborted(tmp_path):
    """The crash is reported, and the process does not abort on the close error.

    A close that fails while the worker unwinds would panic during the
    unwinding, which aborts the process. Such an error is logged instead, so
    the crash is what the process reports, with exit code 1.

    """
    recovery_dir = tmp_path / "rec"
    recovery_dir.mkdir()
    init_db_dir(recovery_dir, 1)
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "unwind",
            str(recovery_dir),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 1, (result.returncode, result.stderr[-3000:])
    assert "simulated crash on 60" in result.stderr, result.stderr[-3000:]


# A probe that reports whether another process holds a recovery partition's lock.
_LOCK_PROBE = """
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1], timeout=0, isolation_level=None)
try:
    conn.execute("BEGIN EXCLUSIVE")
    conn.execute("COMMIT")
    print("free")
except sqlite3.OperationalError:
    print("held")
"""


def _lock_is_held(partition: Path) -> bool:
    """Whether the partition's lock is held, checked from another process.

    Locks of one process are not exclusive to another connection of that
    same process, so the probe must be a separate process.

    """
    out = subprocess.run(
        [sys.executable, "-c", _LOCK_PROBE, f"{partition}.lock"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout.strip()
    return out == "held"


def _holder_and_second_main(
    recovery_dir: str, closing: str, release: str, done: str, go: str, refused: str
) -> None:
    """Run the first execution in a thread, and a second one on its store here.

    The first execution is `_predecessor_main`. The second runs in this
    thread, on the same store, once the test writes `go`, which it does after
    worker 0 has shut down. The second execution's outcome is written to
    `refused`. The first one ends after the test writes `release`.

    """
    errors: List[BaseException] = []

    def first() -> None:
        try:
            _predecessor_main(recovery_dir, closing, release, done)
        except BaseException as err:
            errors.append(err)

    thread = threading.Thread(target=first, daemon=True)
    thread.start()
    _hold_until(Path(go), "the test never started the second execution")
    try:
        run_main(
            _flow(sink=_RecordingSink()),
            epoch_interval=EPOCH_TD,
            recovery_config=RecoveryConfig(recovery_dir),
        )
    except RuntimeError as err:
        cause: BaseException = err
        while cause.__cause__ is not None:
            cause = cause.__cause__
        outcome = f"refused: {cause}"
    else:
        outcome = "ran"
    Path(refused).write_text(outcome, encoding="utf-8")
    thread.join(MARK_TIMEOUT_S)
    if thread.is_alive():
        msg = "the first execution did not end after its close was released"
        raise RuntimeError(msg)
    if errors:
        raise errors[0]


@requires_strict_tables
def test_a_second_execution_on_the_same_directory_keeps_the_first_locks(tmp_path):
    """A second execution that fails on the lock leaves the first one's locks held.

    Both executions run in one child process, on one store. The first has
    two workers: worker 0 has shut down, so its lock is parked for the rest
    of that execution, and worker 1 is held in its close. The second execution
    fails on the lock, and must release only its own locks. The check comes
    from another process, after the second execution has failed.

    """
    recovery_dir = tmp_path / "rec"
    recovery_dir.mkdir()
    init_db_dir(recovery_dir, 1)
    partition = recovery_dir / "part-0.sqlite3"
    closing = tmp_path / "closing"
    release = tmp_path / "release"
    done = tmp_path / "done"
    go = tmp_path / "go"
    refused = tmp_path / "refused"
    log = tmp_path / "holder.log"
    with log.open("w", encoding="utf-8") as out:
        child = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "holder",
                str(recovery_dir),
                str(closing),
                str(release),
                str(done),
                str(go),
                str(refused),
            ],
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    try:
        _await_child(
            child,
            log,
            lambda text: closing.exists() and _WORKER_STOP in text,
            "worker 0 stopped while worker 1 was in its close",
        )
        assert not done.exists(), "the first execution's close had already finished"
        assert _lock_is_held(partition), "the first execution holds no lock"

        go.write_text("go", encoding="utf-8")
        _await_child(
            child,
            log,
            lambda _text: refused.exists(),
            "the second execution reported its outcome",
        )
        outcome = refused.read_text(encoding="utf-8")
        assert outcome.startswith("refused: "), outcome
        assert "in use by another execution" in outcome, outcome
        assert not done.exists(), "the first execution's close had already finished"
        assert _lock_is_held(
            partition
        ), "the second execution released the first one's lock"

        release.write_text("release", encoding="utf-8")
        code = child.wait(timeout=MARK_TIMEOUT_S)
        assert code == 0, _log_text(log)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)
    assert done.exists(), "the first execution's close did not finish"


if __name__ == "__main__":
    if sys.argv[1] == "predecessor":
        setup_tracing(log_level="INFO")
        _predecessor_main(*sys.argv[2:6])
        raise SystemExit(0)
    if sys.argv[1] == "holder":
        setup_tracing(log_level="INFO")
        _holder_and_second_main(*sys.argv[2:8])
        raise SystemExit(0)
    if sys.argv[1] == "unwind":
        _unwind_main(sys.argv[2])
        raise SystemExit(0)
    # One process of the multi-process publish-order test.
    recovery_dir, proc_id, *addresses = sys.argv[1:]
    _reset(Path(recovery_dir))
    cluster_main(
        _flow(),
        addresses,
        int(proc_id),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(recovery_dir, snapshot_interval=SNAPSHOT_TD),
    )
    print(f"CHECKED {len(_checks)}", flush=True)
