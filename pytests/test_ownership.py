"""Exclusive execution lock on the recovery store.

Each recovery partition is locked by the execution that opens it, for as
long as that execution runs. A second execution cannot open the store
while the first is alive. It waits a few seconds for the lock, then
fails without writing anything. The OS releases the lock when the process
exits, even when the process is killed.

This file is also the driver for the subprocess tests. Run as a script
it executes one execution of the flow below, on one worker or several:

    python test_ownership.py RECOVERY_DIR OUT_FILE COUNT WORKERS

"""

import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    FixedPartitionedSource,
    StatefulSourcePartition,
    StatelessSourcePartition,
)
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import mark, raises

# Recovery stores use STRICT tables, which need SQLite 3.37. Python 3.8 on
# Windows bundles an older SQLite, so tests that read a store with
# `sqlite3` skip there.
requires_strict_tables = mark.skipif(
    sqlite3.sqlite_version_info < (3, 37, 0),
    reason=f"sqlite3 {sqlite3.sqlite_version} predates STRICT tables (3.37)",
)


COUNT = 400
EPOCH_TD = timedelta(seconds=0.05)
ITEM_SECONDS = 0.01
# Duplicates after a SIGKILL may only reach back this many items: about
# two epochs of replay at the item rate above, with room to spare.
REPLAY_WINDOW = 20
IS_WINDOWS = sys.platform == "win32"
# Far more input than any test waits for, so the run is still going
# when the test ends it.
LONG_COUNT = 100_000

# Items a predecessor emits before it is frozen.
FREEZE_AFTER = 3
# What a second execution says when another execution holds the lock.
LOCK_ERROR = "in use by another execution"
# Workers per process when two executions start together.
WORKERS = 2
# Seconds allowed for both simultaneous executions to settle.
SIMULTANEOUS_LIMIT = 60
# Seconds the lock winner may keep running after the loser has failed.
GRACE_SECONDS = 15


class _SlowPartition(StatefulSourcePartition):
    """One item per batch after a short sleep, with its position as state."""

    def __init__(self, count, resume_state):
        self._count = count
        self._next = 0 if resume_state is None else resume_state

    def next_batch(self):
        time.sleep(ITEM_SECONDS)
        if self._next >= self._count:
            raise StopIteration()
        item = self._next
        self._next += 1
        return [item]

    def snapshot(self):
        return self._next


class _SlowSource(FixedPartitionedSource):
    def __init__(self, count):
        self._count = count

    def list_parts(self):
        return ["slow"]

    def build_part(self, step_id, for_part, resume_state):
        return _SlowPartition(self._count, resume_state)


class _SlowStatelessPartition(StatelessSourcePartition):
    def __init__(self, count):
        self._count = count
        self._next = 0

    def next_batch(self):
        time.sleep(ITEM_SECONDS)
        if self._next >= self._count:
            raise StopIteration()
        item = self._next
        self._next += 1
        return [item]

    def next_awake(self):
        return None


class _SlowDynamicSource(DynamicSource):
    def __init__(self, count):
        self._count = count

    def build(self, step_id, worker_index, worker_count):
        return _SlowStatelessPartition(self._count)


def _running_total(total, item):
    total = (total or 0) + item
    return (total, (item, total))


def _slow_flow(count, sink):
    flow = Dataflow("ownership")
    s = op.input("inp", flow, _SlowSource(count))
    s = op.key_on("key", s, lambda _item: "ALL")
    s = op.stateful_map("sum", s, _running_total)
    op.output("out", s, sink)
    return flow


class _LineSinkPartition(StatelessSinkPartition):
    def __init__(self, path):
        self._file = open(path, "a", encoding="utf-8")

    def write_batch(self, items):
        for _key, (item, total) in items:
            self._file.write(f"{item} {total}\n")
        # Flush per batch so that a SIGKILL cannot lose emitted lines.
        self._file.flush()


class _LineSink(DynamicSink):
    def __init__(self, path):
        self._path = path

    def build(self, step_id, worker_index, worker_count):
        return _LineSinkPartition(self._path)


def _read_items(path):
    """Items from complete output lines; a line cut short is ignored."""
    if not Path(path).exists():
        return []
    lines = Path(path).read_text(encoding="utf-8").split("\n")[:-1]
    return [int(line.split()[0]) for line in lines]


def _wait_for_items(proc, path, count, timeout=30):
    deadline = time.monotonic() + timeout
    while len(_read_items(path)) < count:
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode()
            msg = f"driver exited with {proc.returncode} early: {stderr}"
            raise AssertionError(msg)
        if time.monotonic() > deadline:
            msg = f"driver emitted fewer than {count} items in {timeout}s"
            raise TimeoutError(msg)
        time.sleep(0.02)


def _kill_if_running(proc):
    if proc.poll() is None:
        proc.kill()
        proc.wait()


def _dynamic_flow(count, sink):
    """Like `_slow_flow`, but every worker reads a partition of its own.

    Each process then has input of its own, so one process cannot finish
    the cluster while another keeps reading.
    """
    flow = Dataflow("ownership_dynamic")
    s = op.input("inp", flow, _SlowDynamicSource(count))
    s = op.key_on("key", s, lambda _item: "ALL")
    s = op.stateful_map("sum", s, _running_total)
    op.output("out", s, sink)
    return flow


def _free_addresses(count):
    socks = [socket.socket() for _ in range(count)]
    try:
        for sock in socks:
            sock.bind(("127.0.0.1", 0))
        return [f"127.0.0.1:{sock.getsockname()[1]}" for sock in socks]
    finally:
        for sock in socks:
            sock.close()


# Runs one `cluster_main` process as a driver. Process 0 and 1 of a
# multi-process cluster are started from here; an empty recovery dir
# means no recovery store.
_CLUSTER_DRIVER = """
import sys

tests_dir, recovery_dir, addresses, proc_id, out_path, count = sys.argv[1:]
sys.path.insert(0, tests_dir)

from bytewax.recovery import RecoveryConfig
from bytewax.testing import cluster_main
from test_ownership import EPOCH_TD, _dynamic_flow, _LineSink

cluster_main(
    _dynamic_flow(int(count), _LineSink(out_path)),
    addresses.split(",") if addresses else [],
    int(proc_id),
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir) if recovery_dir else None,
)
"""


def _spawn_cluster(out_path, proc_id, addresses=(), recovery_dir="", count=LONG_COUNT):
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            _CLUSTER_DRIVER,
            str(Path(__file__).resolve().parent),
            str(recovery_dir),
            ",".join(addresses),
            str(proc_id),
            str(out_path),
            str(count),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _driver_argv(recovery_dir, out_path, count, workers):
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        str(recovery_dir),
        str(out_path),
        str(count),
        str(workers),
    ]


def _spawn_driver(recovery_dir, out_path, count=COUNT, workers=1):
    return subprocess.Popen(
        _driver_argv(recovery_dir, out_path, count, workers),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _run_driver(recovery_dir, out_path, count=COUNT, workers=1):
    return subprocess.run(
        _driver_argv(recovery_dir, out_path, count, workers),
        capture_output=True,
        timeout=120,
        check=False,
    )


def _store_state(recovery_dir):
    """Every progress row in every partition, to show a store is untouched."""
    state = []
    for part in sorted(Path(recovery_dir).glob("part-*.sqlite3")):
        conn = sqlite3.connect(part)
        try:
            for table in ("exs", "fronts", "commits", "snaps"):
                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                state.append((part.name, table, rows))
        finally:
            conn.close()
    return state


def _execution_numbers(recovery_dir):
    numbers = []
    for part in sorted(Path(recovery_dir).glob("part-*.sqlite3")):
        conn = sqlite3.connect(part)
        try:
            numbers += [row[0] for row in conn.execute("SELECT ex_num FROM exs")]
        finally:
            conn.close()
    return sorted(set(numbers))


@mark.skipif(IS_WINDOWS, reason="SIGSTOP is not available on Windows")
@requires_strict_tables
def test_stopped_predecessor_blocks_successor_until_killed(tmp_path):
    init_db_dir(tmp_path, 1)
    first_out = tmp_path / "first.txt"
    predecessor = _spawn_driver(tmp_path, first_out)
    try:
        _wait_for_items(predecessor, first_out, FREEZE_AFTER)
        # Frozen, not dead: it still holds the store's lock.
        predecessor.send_signal(signal.SIGSTOP)
        before = _store_state(tmp_path)

        successor_out = tmp_path / "successor.txt"
        started = time.monotonic()
        successor = _run_driver(tmp_path, successor_out)
        elapsed = time.monotonic() - started
        assert successor.returncode != 0
        assert LOCK_ERROR in successor.stderr.decode()
        # The wait for the lock is bounded by the busy timeout.
        assert elapsed < 30
        # The refused successor wrote nothing and emitted nothing.
        assert _store_state(tmp_path) == before
        assert _read_items(successor_out) == []

        predecessor.send_signal(signal.SIGKILL)
        predecessor.communicate(timeout=60)
    finally:
        _kill_if_running(predecessor)

    # The lock went with the process, so a successor starts at once and
    # resumes from the store with no loss.
    resumed_out = tmp_path / "resumed.txt"
    resumed = _run_driver(tmp_path, resumed_out)
    assert resumed.returncode == 0, resumed.stderr.decode()
    first = _read_items(first_out)
    emitted = Counter(first + _read_items(resumed_out))
    assert set(emitted) >= set(range(COUNT))
    # Only items near the kill may be replayed.
    replay_floor = max(first) - REPLAY_WINDOW
    assert all(item >= replay_floor for item, count in emitted.items() if count > 1)


@mark.skipif(IS_WINDOWS, reason="SIGSTOP is not available on Windows")
def test_resumed_predecessor_finishes_alone(tmp_path):
    init_db_dir(tmp_path, 1)
    out = tmp_path / "predecessor.txt"
    predecessor = _spawn_driver(tmp_path, out)
    try:
        _wait_for_items(predecessor, out, FREEZE_AFTER)
        predecessor.send_signal(signal.SIGSTOP)
        successor = _run_driver(tmp_path, tmp_path / "successor.txt")
        assert successor.returncode != 0
        assert LOCK_ERROR in successor.stderr.decode()

        predecessor.send_signal(signal.SIGCONT)
        _stdout, stderr = predecessor.communicate(timeout=120)
    finally:
        _kill_if_running(predecessor)
    assert predecessor.returncode == 0, stderr.decode()
    # One execution ran alone, so every item came out once, in order.
    assert _read_items(out) == list(range(COUNT))


def test_simultaneous_executions_never_both_run(tmp_path):
    # Four partitions, and two workers in each of two processes. Each
    # process locks the partitions its own workers are primary for, and
    # neither can resume without all of them. So one must fail on a busy
    # lock, and neither may hang.
    init_db_dir(tmp_path, 4)
    outs = [tmp_path / "first.txt", tmp_path / "second.txt"]
    procs = [
        _spawn_driver(tmp_path, out, count=LONG_COUNT, workers=WORKERS) for out in outs
    ]
    try:
        deadline = time.monotonic() + SIMULTANEOUS_LIMIT
        first_exit = None
        while any(proc.poll() is None for proc in procs):
            now = time.monotonic()
            if first_exit is None and any(proc.poll() is not None for proc in procs):
                first_exit = now
            # The winner keeps running, so stop waiting a while after the
            # first exit rather than for both.
            if first_exit is not None and now - first_exit > GRACE_SECONDS:
                break
            if now > deadline:
                msg = "no simultaneous execution settled in time"
                raise AssertionError(msg)
            time.sleep(0.05)
        running = [proc for proc in procs if proc.poll() is None]
        # Never both: at most the execution holding the locks still runs.
        assert len(running) <= 1
    finally:
        for proc in procs:
            _kill_if_running(proc)

    failed = [proc for proc in procs if proc not in running]
    assert failed
    errors = []
    for proc in failed:
        assert proc.returncode != 0
        errors.append(proc.stderr.read().decode())
    assert any(LOCK_ERROR in error for error in errors), errors
    # Only one execution can have resumed, so only one wrote output.
    assert len([out for out in outs if _read_items(out)]) <= 1


def test_second_execution_in_one_process_is_refused(tmp_path):
    # SQLite's lock also refuses a second connection in the same process.
    init_db_dir(tmp_path, 1)
    entered = threading.Event()
    gate = threading.Event()

    def hold(item):
        entered.set()
        gate.wait(timeout=60)
        return item

    flow = Dataflow("holding")
    s = op.input("inp", flow, TestingSource(range(3)))
    s = op.map("hold", s, hold)
    op.output("out", s, TestingSink([]))
    runner = threading.Thread(
        target=run_main,
        args=(flow,),
        kwargs={
            "epoch_interval": EPOCH_TD,
            "recovery_config": RecoveryConfig(str(tmp_path)),
        },
    )
    runner.start()
    try:
        assert entered.wait(timeout=30)
        with raises(RuntimeError, match=LOCK_ERROR):
            run_main(
                _slow_flow(5, TestingSink([])),
                epoch_interval=EPOCH_TD,
                recovery_config=RecoveryConfig(str(tmp_path)),
            )
    finally:
        gate.set()
        runner.join(timeout=60)
    assert not runner.is_alive()


@requires_strict_tables
def test_workers_of_one_process_open_each_partition_once(tmp_path):
    # Four partitions and four workers in one process. A partition is
    # opened by its primary worker only. A sibling that also opened it
    # would be refused by this process's own lock.
    init_db_dir(tmp_path, 4)
    recovery_config = RecoveryConfig(str(tmp_path))
    out = []
    flow = _slow_flow(20, TestingSink(out))
    cluster_main(
        flow,
        [],
        0,
        worker_count_per_proc=4,
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    assert sorted(item for _key, (item, _total) in out) == list(range(20))

    # The same process resumes the store once the first execution is over.
    cluster_main(
        flow,
        [],
        0,
        worker_count_per_proc=4,
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    assert _execution_numbers(tmp_path) == [0, 1]


def test_lock_is_a_sidecar_file(tmp_path):
    init_db_dir(tmp_path, 1)
    run_main(
        _slow_flow(5, TestingSink([])),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(str(tmp_path)),
    )
    # The lock is a file beside the partition, and SQLite leaves no
    # journal or WAL file beside it.
    assert (tmp_path / "part-0.sqlite3.lock").exists()
    assert not (tmp_path / "part-0.sqlite3.lock-journal").exists()
    assert not (tmp_path / "part-0.sqlite3.lock-wal").exists()


# Holds the lock of a recovery partition until it is killed, the way a
# running execution does.
_LOCK_HOLDER_CODE = """
import sqlite3
import sys
import time

conn = sqlite3.connect(sys.argv[1] + ".lock", isolation_level=None)
conn.execute("PRAGMA locking_mode = EXCLUSIVE")
conn.execute("PRAGMA journal_mode = MEMORY")
conn.executescript("BEGIN EXCLUSIVE; COMMIT;")
print("locked", flush=True)
time.sleep(600)
"""


@requires_strict_tables
def test_refused_execution_leaves_older_store_untouched(tmp_path):
    # Simulate a store last written by an older version, which has one
    # migration fewer: no `snaps` table, and `user_version` 4.
    init_db_dir(tmp_path, 1)
    part = tmp_path / "part-0.sqlite3"
    conn = sqlite3.connect(part)
    conn.executescript("DROP TABLE snaps; PRAGMA user_version = 4;")
    conn.close()
    # Hold the partition's lock from another process, as a running
    # execution would. A second connection in this process could share
    # SQLite's lock state, so it would not be refused.
    holder = subprocess.Popen(
        [sys.executable, "-c", _LOCK_HOLDER_CODE, str(part)],
        stdout=subprocess.PIPE,
    )
    before = None
    # Count Python ticks while the refused execution waits for the lock.
    ticks = [0]
    stop = threading.Event()

    def tick():
        while not stop.is_set():
            ticks[0] += 1
            time.sleep(0.001)

    ticker = threading.Thread(target=tick)
    try:
        assert holder.stdout.readline().strip() == b"locked"
        before = part.read_bytes()
        ticker.start()
        with raises(RuntimeError, match=LOCK_ERROR):
            run_main(
                _slow_flow(5, TestingSink([])),
                epoch_interval=EPOCH_TD,
                recovery_config=RecoveryConfig(str(tmp_path)),
            )
    finally:
        stop.set()
        ticker.join(timeout=10)
        holder.kill()
        holder.wait()

    # The refused execution neither migrated nor wrote anything.
    conn = sqlite3.connect(part)
    try:
        assert conn.execute("PRAGMA user_version").fetchone() == (4,)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        conn.close()
    assert ("snaps",) not in tables
    assert part.read_bytes() == before
    # Waiting for the lock did not hold the GIL, so other threads ran.
    assert ticks[0] > 100


@requires_strict_tables
def test_execution_numbers_follow_the_newest(tmp_path):
    init_db_dir(tmp_path, 2)
    recovery_config = RecoveryConfig(str(tmp_path))
    for _ in range(2):
        run_main(
            _slow_flow(5, TestingSink([])),
            epoch_interval=EPOCH_TD,
            recovery_config=recovery_config,
        )
    # Each execution is numbered one past the newest one in the store.
    assert _execution_numbers(tmp_path) == [0, 1]


def test_two_processes_share_one_store(tmp_path):
    # One directory, two processes, two partitions. Each partition is
    # locked by its own primary worker, so each process locks only its own
    # partition. Neither may refuse the other.
    init_db_dir(tmp_path, 2)
    addresses = _free_addresses(2)
    outs = [tmp_path / "p0.txt", tmp_path / "p1.txt"]
    procs = [
        _spawn_cluster(out, proc_id, addresses, tmp_path, COUNT)
        for proc_id, out in enumerate(outs)
    ]
    try:
        results = [proc.communicate(timeout=120) for proc in procs]
    finally:
        for proc in procs:
            _kill_if_running(proc)

    for proc, (_stdout, stderr) in zip(procs, results):
        assert proc.returncode == 0, stderr.decode()
    # The stateful key routes all output to one process, so the two
    # outputs are summed: every input item of both processes, once.
    assert sum(len(_read_items(out)) for out in outs) == 2 * COUNT


def test_shared_store_single_process(entry_point, tmp_path):
    # Two partitions in one process, and a second execution on the same
    # store.
    init_db_dir(tmp_path, 2)
    recovery_config = RecoveryConfig(str(tmp_path))
    out = []
    flow = _slow_flow(20, TestingSink(out))

    entry_point(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    assert sorted(item for _key, (item, _total) in out) == list(range(20))

    entry_point(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)


if __name__ == "__main__":
    recovery_dir, out_file = sys.argv[1], sys.argv[2]
    count, workers = int(sys.argv[3]), int(sys.argv[4])
    driver_flow = _slow_flow(count, _LineSink(out_file))
    if workers > 1:
        cluster_main(
            driver_flow,
            [],
            0,
            worker_count_per_proc=workers,
            epoch_interval=EPOCH_TD,
            recovery_config=RecoveryConfig(recovery_dir),
        )
    else:
        run_main(
            driver_flow,
            epoch_interval=EPOCH_TD,
            recovery_config=RecoveryConfig(recovery_dir),
        )
    print(f"emitted {len(_read_items(out_file))}")
