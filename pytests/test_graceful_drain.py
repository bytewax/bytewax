"""Graceful drain: an external shutdown request ends an execution at a
clean, exactly resumable point.

A drain is handled as EOF by every input, so each partition snapshots
its exact position and the execution closes through the normal EOF
path. Stateful logic is paused, not finalized: a drain-induced EOF
does not run `on_eof`, so windows and folds keep their state for the
next execution. A clean close is logged at `INFO` as `Execution N
closed cleanly`. An execution that is killed, aborted, or crashes must
not log it.

This file is also the driver for the subprocess tests. Run as a script
it executes one execution of the flow below:

    python test_graceful_drain.py RECOVERY_DIR OUT_FILE COUNT
        [--drain-on-sigterm] [--log-info]

With `--drain-on-sigterm` it first installs the SIGTERM handler from the
`request_graceful_drain` docstring. With `--log-info` it logs at `INFO`
to stdout.

"""

import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from itertools import accumulate
from pathlib import Path
from typing import List, Optional

import bytewax.operators as op
import bytewax.run as bytewax_run
from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    FixedPartitionedSource,
    StatefulSourcePartition,
    StatelessSourcePartition,
)
from bytewax.operators.windowing import EventClock, TumblingWindower, collect_window
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from bytewax.tracing import setup_tracing
from pytest import fixture, mark, raises
from test_ownership import LOCK_ERROR, _run_driver

COUNT = 400
EPOCH_TD = timedelta(seconds=0.05)
ITEM_SECONDS = 0.01
CRASH_AT = 5
# Duplicates after a SIGKILL may only reach back this many items: about
# two epochs of replay at the item rate above, with room to spare.
REPLAY_WINDOW = 20
IS_WINDOWS = sys.platform == "win32"


# Items every `_SlowPartition` has read, so a test can request a drain
# after real progress instead of after a wall-clock delay.
_ITEMS_READ = [0]


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
        _ITEMS_READ[0] += 1
        return [item]

    def snapshot(self):
        return self._next


# The resume state each `_SlowSource` partition was built with, so a test
# can see where a drained execution stopped reading.
_RESUMED_FROM: List[Optional[int]] = []


class _SlowSource(FixedPartitionedSource):
    def __init__(self, count):
        self._count = count

    def list_parts(self):
        return ["slow"]

    def build_part(self, step_id, for_part, resume_state):
        _RESUMED_FROM.append(resume_state)
        return _SlowPartition(self._count, resume_state)


class _SleepyPartition(StatefulSourcePartition):
    """Emits one item, then sleeps far longer than any test runs."""

    def __init__(self):
        self._sent = False
        self._wake = None

    def next_batch(self):
        if self._sent:
            raise StopIteration()
        self._sent = True
        self._wake = datetime.now(tz=timezone.utc) + timedelta(seconds=20)
        return [0]

    def next_awake(self):
        return self._wake

    def snapshot(self):
        return self._sent


class _SleepySource(FixedPartitionedSource):
    def list_parts(self):
        return ["sleepy"]

    def build_part(self, step_id, for_part, resume_state):
        return _SleepyPartition()


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
    flow = Dataflow("graceful_drain")
    s = op.input("inp", flow, _SlowSource(count))
    s = op.key_on("key", s, lambda _item: "ALL")
    s = op.stateful_map("sum", s, _running_total)
    op.output("out", s, sink)
    return flow


def _plain_flow(source, out, mapper=None):
    flow = Dataflow("unclean_exit")
    s = op.input("inp", flow, source)
    if mapper is not None:
        s = op.map("map", s, mapper)
    op.output("out", s, TestingSink(out))
    return flow


def _crash_once(crashed):
    def mapper(item):
        if item == CRASH_AT and not crashed[0]:
            crashed[0] = True
            msg = "simulated crash"
            raise RuntimeError(msg)
        return item

    return mapper


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


def _drain_on_sigterm(signum, frame):
    # The handler from the `request_graceful_drain` docstring. A second
    # SIGTERM ends the process the usual way.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    bytewax_run.request_graceful_drain()


def _run_with_drain(flow, recovery_config, after=None, run=run_main, after_items=None):
    """Run the flow and request a graceful drain during it.

    The request comes `after` seconds in, or, with `after_items`, once the
    flow's `_SlowPartition`s have read that many items, which does not
    depend on how fast the machine is.
    """
    if after_items is None:
        timer = threading.Timer(after, bytewax_run.request_graceful_drain)
        timer.start()
        try:
            run(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
        finally:
            timer.cancel()
        return

    _ITEMS_READ[0] = 0
    stop = threading.Event()

    def request_after_progress():
        while not stop.wait(0.005):
            if _ITEMS_READ[0] >= after_items:
                bytewax_run.request_graceful_drain()
                return

    watcher = threading.Thread(target=request_after_progress, daemon=True)
    watcher.start()
    try:
        run(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    finally:
        stop.set()
        watcher.join()


def _spawn(recovery_dir, out_path, count, drain_on_sigterm=False, log_info=False):
    argv = [
        sys.executable,
        str(Path(__file__).resolve()),
        str(recovery_dir),
        str(out_path),
        str(count),
    ]
    if drain_on_sigterm:
        argv.append("--drain-on-sigterm")
    if log_info:
        argv.append("--log-info")
    return subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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


def _assert_no_drain_pending():
    # Refused with RuntimeError while a drain request is pending.
    try:
        run_main(_plain_flow(TestingSource([]), []))
    except RuntimeError as err:
        msg = f"a drain request was left pending by an earlier test: {err}"
        raise AssertionError(msg) from err


@fixture(autouse=True)
def _no_drain_request_left_over():
    # A request made while no execution runs waits for the next execution
    # in the process, so one left by an earlier test would drain this
    # test's run. Fail on it at both ends rather than clear it.
    _assert_no_drain_pending()
    yield
    _assert_no_drain_pending()


def test_programmatic_drain_stops_cleanly(tmp_path):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))
    out = []
    flow = _slow_flow(COUNT, TestingSink(out))

    _run_with_drain(flow, recovery_config, after_items=20)

    # Fewer items than the input holds prove the drain ended the run.
    assert 0 < len(out) < COUNT


@mark.skipif(IS_WINDOWS, reason="SIGTERM is not catchable on Windows")
def test_sigterm_without_handler_keeps_default_action(tmp_path):
    # With no Python handler, SIGTERM ends the process by signal even with
    # a recovery config, and the execution does not close cleanly.
    init_db_dir(tmp_path, 1)
    out_path = tmp_path / "out.txt"
    proc = _spawn(tmp_path, out_path, COUNT)
    try:
        _wait_for_items(proc, out_path, 3)
        proc.send_signal(signal.SIGTERM)
        _stdout, stderr = proc.communicate(timeout=60)
    finally:
        _kill_if_running(proc)

    assert proc.returncode == -signal.SIGTERM, stderr.decode()


@mark.skipif(IS_WINDOWS, reason="SIGTERM is not catchable on Windows")
def test_sigterm_with_drain_handler_drains_to_clean_point(tmp_path):
    # The handler is installed before the execution starts, so the SIGTERM
    # drains it. A second execution on the same store then emits every
    # item exactly once across both runs.
    init_db_dir(tmp_path, 1)
    first_out = tmp_path / "first.txt"
    proc = _spawn(tmp_path, first_out, COUNT, drain_on_sigterm=True)
    try:
        _wait_for_items(proc, first_out, 3)
        proc.send_signal(signal.SIGTERM)
        _stdout, stderr = proc.communicate(timeout=60)
    finally:
        _kill_if_running(proc)

    assert proc.returncode == 0, stderr.decode()
    assert 0 < len(_read_items(first_out)) < COUNT

    second_out = tmp_path / "second.txt"
    resumed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(tmp_path),
            str(second_out),
            str(COUNT),
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert resumed.returncode == 0, resumed.stderr.decode()
    emitted = _read_items(first_out) + _read_items(second_out)
    assert emitted == list(range(COUNT))


# Input item at which the predecessor stalls inside an input call.
STALL_AT = 5
# File whose existence releases the stalled call. The predecessor's
# script sets it; the test creates it when the predecessor may finish.
STALL_RELEASE = None


class _StallPartition(_SlowPartition):
    def next_batch(self):
        if self._next == STALL_AT:
            print("stalled", flush=True)
            # A drain waits for this call, so the predecessor stays
            # alive and draining until the test releases it.
            deadline = time.monotonic() + 120
            while not STALL_RELEASE.exists():
                assert time.monotonic() < deadline, "stall never released"
                time.sleep(0.01)
        return super().next_batch()


class _StallSource(_SlowSource):
    """Like `_SlowSource`, but one input call stalls at `STALL_AT`."""

    def build_part(self, step_id, for_part, resume_state):
        return _StallPartition(self._count, resume_state)


def _stall_flow(count, sink):
    """Same flow and step IDs as the driver in `test_ownership.py`."""
    flow = Dataflow("ownership")
    s = op.input("inp", flow, _StallSource(count))
    s = op.key_on("key", s, lambda _item: "ALL")
    s = op.stateful_map("sum", s, _running_total)
    op.output("out", s, sink)
    return flow


# Runs one execution that stalls at `STALL_AT` and drains on SIGTERM,
# with the handler from the `request_graceful_drain` docstring.
_STALL_CODE = """
import signal
import sys
from pathlib import Path

tests_dir, recovery_dir, out_file, count, release, draining = sys.argv[1:]
sys.path.insert(0, tests_dir)

import test_graceful_drain
from bytewax.recovery import RecoveryConfig
from bytewax.testing import run_main
from test_graceful_drain import EPOCH_TD, _drain_on_sigterm, _LineSink, _stall_flow

test_graceful_drain.STALL_RELEASE = Path(release)


def _on_sigterm(signum, frame):
    _drain_on_sigterm(signum, frame)
    # The drain is requested now; tell the test while this process is alive.
    Path(draining).touch()


signal.signal(signal.SIGTERM, _on_sigterm)
run_main(
    _stall_flow(int(count), _LineSink(out_file)),
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir),
)
"""


@mark.skipif(IS_WINDOWS, reason="SIGTERM is not catchable on Windows")
def test_successor_during_drain_fails_then_resumes_without_replay(tmp_path):
    init_db_dir(tmp_path, 1)
    first_out = tmp_path / "first.txt"
    release = tmp_path / "release"
    draining = tmp_path / "draining"
    predecessor = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _STALL_CODE,
            str(Path(__file__).resolve().parent),
            str(tmp_path),
            str(first_out),
            str(COUNT),
            str(release),
            str(draining),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert predecessor.stdout.readline().strip() == b"stalled"
        predecessor.send_signal(signal.SIGTERM)
        # Precondition: the predecessor is alive and inside its drain,
        # still holding its lock, because its stalled call is not
        # released yet.
        deadline = time.monotonic() + 30
        while not draining.exists():
            assert predecessor.poll() is None, "predecessor exited before draining"
            assert time.monotonic() < deadline, "predecessor never began draining"
            time.sleep(0.01)
        assert predecessor.poll() is None
        assert not release.exists()

        successor_out = tmp_path / "successor.txt"
        successor = _run_driver(tmp_path, successor_out)
        assert successor.returncode != 0
        assert LOCK_ERROR in successor.stderr.decode()
        assert _read_items(successor_out) == []
        assert predecessor.poll() is None

        release.touch()
        _stdout, stderr = predecessor.communicate(timeout=120)
    finally:
        release.touch()
        _kill_if_running(predecessor)
    assert predecessor.returncode == 0, stderr.decode()

    # Once the predecessor has drained and exited, a retry resumes the
    # store, and every item comes out exactly once across both runs.
    retry_out = tmp_path / "retry.txt"
    retry = _run_driver(tmp_path, retry_out)
    assert retry.returncode == 0, retry.stderr.decode()
    assert _read_items(first_out) + _read_items(retry_out) == list(range(COUNT))


def test_handoff_zero_replay(tmp_path):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))
    out = []
    flow = _slow_flow(COUNT, TestingSink(out))

    _run_with_drain(flow, recovery_config, after_items=40)
    first = list(out)
    out.clear()
    assert 0 < len(first) < COUNT

    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    second = list(out)

    # Each item exactly once, in order, across the two executions.
    emitted = first + second
    assert [item for _key, (item, _total) in emitted] == list(range(COUNT))
    # The running totals continue across the handoff with no double
    # counting.
    assert [total for _key, (_item, total) in emitted] == list(accumulate(range(COUNT)))


@mark.skipif(IS_WINDOWS, reason="SIGKILL is not available on Windows")
def test_kill9_resumes_without_loss(tmp_path):
    init_db_dir(tmp_path, 1)
    first_out = tmp_path / "first.txt"
    proc = _spawn(tmp_path, first_out, COUNT)
    try:
        _wait_for_items(proc, first_out, 5)
        proc.send_signal(signal.SIGKILL)
        proc.communicate(timeout=60)
    finally:
        _kill_if_running(proc)

    assert proc.returncode != 0

    second_out = tmp_path / "second.txt"
    resumed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(tmp_path),
            str(second_out),
            str(COUNT),
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert resumed.returncode == 0, resumed.stderr.decode()

    # Zero loss: every item is emitted at least once across both runs.
    first = _read_items(first_out)
    second = _read_items(second_out)
    emitted = Counter(first + second)
    assert set(emitted) >= set(range(COUNT))
    # Bounded duplicates: only items near the crash may be replayed.
    replay_floor = max(first) - REPLAY_WINDOW
    assert all(item >= replay_floor for item, count in emitted.items() if count > 1)


def test_drain_interrupts_long_sleep(tmp_path):
    # A partition asleep for 20 seconds must still see the drain.
    init_db_dir(tmp_path, 1)
    out = []
    flow = Dataflow("sleepy")
    s = op.input("inp", flow, _SleepySource())
    op.output("out", s, TestingSink(out))

    started = time.monotonic()
    _run_with_drain(flow, RecoveryConfig(str(tmp_path)), after=0.5)
    elapsed = time.monotonic() - started

    assert elapsed < 10
    assert out == [0]


class _SleepyStatelessPartition(StatelessSourcePartition):
    """Emits one item, then sleeps far longer than any test runs."""

    def __init__(self):
        self._sent = False
        self._wake = None

    def next_batch(self):
        if self._sent:
            raise StopIteration()
        self._sent = True
        self._wake = datetime.now(tz=timezone.utc) + timedelta(seconds=20)
        return [0]

    def next_awake(self):
        return self._wake


class _SleepyDynamicSource(DynamicSource):
    def build(self, step_id, worker_index, worker_count):
        return _SleepyStatelessPartition()


def test_drain_interrupts_long_sleep_dynamic(tmp_path):
    # The DynamicSource counterpart of `test_drain_interrupts_long_sleep`.
    init_db_dir(tmp_path, 1)
    out = []
    flow = Dataflow("sleepy_dynamic")
    s = op.input("inp", flow, _SleepyDynamicSource())
    op.output("out", s, TestingSink(out))

    started = time.monotonic()
    _run_with_drain(flow, RecoveryConfig(str(tmp_path)), after=0.5)
    elapsed = time.monotonic() - started

    assert elapsed < 10
    assert out == [0]


class _EarlyThenSleepy(StatelessSourcePartition):
    """Emits one item at once, then sleeps far longer than any test runs.

    The second call returns nothing, so the partition advances its epoch
    and then waits for its far awake time.
    """

    def __init__(self):
        self._calls = 0
        self._wake = None

    def next_batch(self):
        self._calls += 1
        if self._calls == 1:
            self._wake = datetime.now(tz=timezone.utc) + timedelta(seconds=0.1)
            return [0]
        if self._calls == 2:
            self._wake = datetime.now(tz=timezone.utc) + timedelta(seconds=20)
            return []
        raise StopIteration()

    def next_awake(self):
        return self._wake


class _OneItem(StatelessSourcePartition):
    def __init__(self):
        self._sent = False

    def next_batch(self):
        if self._sent:
            raise StopIteration()
        self._sent = True
        return [1]

    def next_awake(self):
        return None


class _SplitSource(DynamicSource):
    """Worker 0 sleeps after its epoch advances; every other worker is quick."""

    def build(self, step_id, worker_index, worker_count):
        if worker_index == 0:
            return _EarlyThenSleepy()
        return _OneItem()


def _slow_for_one(item):
    # Holds the probe back on the other worker, so worker 0's partition
    # is blocked when a drain wakes it.
    if item == 1:
        time.sleep(1.0)
    return item


def _two_workers(flow, **kwargs):
    return cluster_main(flow, [], 0, worker_count_per_proc=2, **kwargs)


def test_drain_reaches_partition_blocked_by_slow_worker(tmp_path):
    # Worker 0's partition advances its epoch and then sleeps for 20
    # seconds, while worker 1's slow map holds the probe behind that
    # epoch. A drain that finds worker 0's partition blocked must still
    # reach it once worker 1 catches up, not when its awake time comes.
    init_db_dir(tmp_path, 1)
    out = []
    flow = Dataflow("blocked_long_sleep")
    s = op.input("inp", flow, _SplitSource())
    s = op.map("slow", s, _slow_for_one)
    op.output("out", s, TestingSink(out))

    started = time.monotonic()
    _run_with_drain(flow, RecoveryConfig(str(tmp_path)), after=0.5, run=_two_workers)
    elapsed = time.monotonic() - started

    assert elapsed < 10
    assert sorted(out) == [0, 1]


def test_drain_stops_stateless_source(tmp_path):
    init_db_dir(tmp_path, 1)
    out = []
    flow = Dataflow("stateless")
    s = op.input("inp", flow, _SlowDynamicSource(COUNT))
    op.output("out", s, TestingSink(out))

    _run_with_drain(flow, RecoveryConfig(str(tmp_path)), after=0.5)

    assert 0 < len(out) < COUNT
    assert out == list(range(len(out)))


def test_request_before_execution_drains_the_next_one(tmp_path):
    # A request made while no execution runs waits for the next execution
    # in this process, which then drains as soon as it starts.
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))
    first = []
    flow = _slow_flow(COUNT, TestingSink(first))

    bytewax_run.request_graceful_drain()
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)

    assert len(first) < COUNT

    # The request was consumed by that execution, so the store resumes
    # with no replayed items.
    second = []
    run_main(
        _slow_flow(COUNT, TestingSink(second)),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    emitted = first + second
    assert [item for _key, (item, _total) in emitted] == list(range(COUNT))


def test_request_drain_without_recovery_raises(entry_point):
    # A drain discards open windows and folds when there is no recovery
    # store, so asking for one there must fail rather than lose state.
    errors = []

    def ask(item):
        try:
            bytewax_run.request_graceful_drain()
        except RuntimeError as err:
            errors.append(str(err))
        return item

    out = []
    flow = Dataflow("drain_without_recovery")
    s = op.input("inp", flow, TestingSource(range(3)))
    s = op.map("ask", s, ask)
    op.output("out", s, TestingSink(out))
    entry_point(flow, epoch_interval=EPOCH_TD)

    assert errors
    assert all("recovery" in msg for msg in errors), errors
    assert out == [0, 1, 2]


def test_pending_drain_refuses_execution_without_recovery(tmp_path):
    # A drain pending in a process belongs to the execution running it.
    # An execution without recovery must not start meanwhile, or the
    # drain would discard its open windows and folds.
    init_db_dir(tmp_path, 1)
    entered = threading.Event()
    gate = threading.Event()

    def hold(item):
        entered.set()
        gate.wait(timeout=30)
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
        bytewax_run.request_graceful_drain()
        with raises(RuntimeError, match="drain is pending"):
            run_main(_slow_flow(5, TestingSink([])), epoch_interval=EPOCH_TD)
    finally:
        gate.set()
        runner.join(timeout=60)
    assert not runner.is_alive()


def test_pending_request_refuses_run_without_recovery_and_stays_pending(tmp_path):
    # Refusing the run must not consume the request, so the next execution
    # with a recovery config still drains as soon as it starts.
    init_db_dir(tmp_path, 1)
    bytewax_run.request_graceful_drain()
    with raises(RuntimeError, match="drain is pending"):
        run_main(_slow_flow(5, TestingSink([])), epoch_interval=EPOCH_TD)

    out = []
    run_main(
        _slow_flow(COUNT, TestingSink(out)),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(str(tmp_path)),
    )

    assert len(out) < COUNT


# Runs one process of a two-process cluster whose step asks for a drain.
# A refused request is printed, so the test can see it on stdout.
_MULTI_PROCESS_REQUEST_CODE = """
import sys

tests_dir, recovery_dir, addresses, proc_id = sys.argv[1:]
sys.path.insert(0, tests_dir)

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import request_graceful_drain
from bytewax.testing import TestingSink, cluster_main
from test_graceful_drain import EPOCH_TD, _SlowDynamicSource


def ask(item):
    try:
        request_graceful_drain()
    except RuntimeError:
        print("refused", flush=True)
    return item


flow = Dataflow("multi_process_request")
s = op.input("inp", flow, _SlowDynamicSource(20))
s = op.map("ask", s, ask)
op.output("out", s, TestingSink([]))
cluster_main(
    flow,
    addresses.split(","),
    int(proc_id),
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir),
)
"""


def test_request_drain_in_multi_process_cluster_raises(tmp_path):
    # A cluster's processes would not agree on the end of input, so a
    # drain in one would finish the others early. Each process refuses.
    init_db_dir(tmp_path, 2)
    addresses = _free_addresses(2)
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _MULTI_PROCESS_REQUEST_CODE,
                str(Path(__file__).resolve().parent),
                str(tmp_path),
                ",".join(addresses),
                str(proc_id),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for proc_id in range(2)
    ]
    try:
        outputs = [proc.communicate(timeout=120) for proc in procs]
    finally:
        for proc in procs:
            _kill_if_running(proc)

    for proc, (stdout, stderr) in zip(procs, outputs):
        assert proc.returncode == 0, stderr.decode()
        assert "refused" in stdout.decode(), stdout.decode()


# Starts a two-process cluster in a process with a drain request pending.
_PENDING_CLUSTER_CODE = """
import sys

tests_dir, recovery_dir, addresses, proc_id = sys.argv[1:]
sys.path.insert(0, tests_dir)

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import request_graceful_drain
from bytewax.testing import TestingSink, TestingSource, cluster_main
from test_graceful_drain import EPOCH_TD

flow = Dataflow("pending_cluster")
s = op.input("inp", flow, TestingSource(range(3)))
op.output("out", s, TestingSink([]))
request_graceful_drain()
cluster_main(
    flow,
    addresses.split(","),
    int(proc_id),
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir),
)
"""


def test_pending_request_refuses_multi_process_start(tmp_path):
    # The peer process never starts here, so a cluster that does start
    # would block; the refusal must come before it connects.
    init_db_dir(tmp_path, 2)
    addresses = _free_addresses(2)
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            _PENDING_CLUSTER_CODE,
            str(Path(__file__).resolve().parent),
            str(tmp_path),
            ",".join(addresses),
            "0",
        ],
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode != 0
    assert "drain is pending" in proc.stderr.decode(), proc.stderr.decode()


# One window spans every item, so an uninterrupted run emits one window.
WINDOW_BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)
WINDOW_COUNT = 120
# Far more input than any test waits for, so these runs end by drain.
LONG_COUNT = 100_000


def _window_flow(sink):
    flow = Dataflow("graceful_drain_window")
    s = op.input("inp", flow, _SlowSource(WINDOW_COUNT))
    s = op.key_on("key", s, lambda _item: "ALL")
    clock = EventClock(
        ts_getter=lambda item: WINDOW_BASE + timedelta(seconds=item),
        wait_for_system_duration=timedelta(days=1),
    )
    windower = TumblingWindower(length=timedelta(seconds=1000), align_to=WINDOW_BASE)
    windows = collect_window("window", s, clock, windower)
    op.output("out", windows.down, sink)
    return flow


def _fold_flow(sink):
    flow = Dataflow("graceful_drain_fold")
    s = op.input("inp", flow, _SlowSource(WINDOW_COUNT))
    s = op.key_on("key", s, lambda _item: "ALL")
    total = op.fold_final("total", s, int, lambda acc, item: acc + item)
    op.output("out", total, sink)
    return flow


# With a snapshot interval, a drain can stop between snapshot boundaries.
SNAPSHOT_TD = EPOCH_TD * 3


def _uninterrupted(entry_point, make_flow, store_dir, snapshot_interval=None):
    store_dir.mkdir(parents=True)
    init_db_dir(store_dir, 1)
    out = []
    entry_point(
        make_flow(TestingSink(out)),
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(
            str(store_dir), snapshot_interval=snapshot_interval
        ),
    )
    return out


def _drained_then_resumed(entry_point, make_flow, store_dir, snapshot_interval=None):
    """Drain the first execution mid-run, then resume to EOF.

    Returns what each execution emitted, and the input position the second
    execution resumed from, which is where the drain stopped the first.
    """
    store_dir.mkdir(parents=True)
    init_db_dir(store_dir, 1)
    recovery_config = RecoveryConfig(
        str(store_dir), snapshot_interval=snapshot_interval
    )
    first = []
    _run_with_drain(
        make_flow(TestingSink(first)),
        recovery_config,
        run=entry_point,
        after_items=20,
    )

    second = []
    _RESUMED_FROM.clear()
    entry_point(
        make_flow(TestingSink(second)),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    (resumed_from,) = _RESUMED_FROM
    return first, second, resumed_from


def test_drain_pauses_window_until_resume(entry_point, tmp_path):
    expected = _uninterrupted(entry_point, _window_flow, tmp_path / "reference")
    # The single window holds every item, in order.
    assert expected == [("ALL", (0, list(range(WINDOW_COUNT))))]

    first, second, resumed_from = _drained_then_resumed(
        entry_point, _window_flow, tmp_path / "drained"
    )
    # The drain stopped the first execution partway through the input.
    assert resumed_from is not None and 0 < resumed_from < WINDOW_COUNT
    # The window is emitted once, with every item, across the handoff.
    assert first + second == expected


def test_drain_pauses_fold_final_until_resume(entry_point, tmp_path):
    expected = _uninterrupted(entry_point, _fold_flow, tmp_path / "reference")
    assert expected == [("ALL", sum(range(WINDOW_COUNT)))]

    first, second, resumed_from = _drained_then_resumed(
        entry_point, _fold_flow, tmp_path / "drained"
    )
    assert resumed_from is not None and 0 < resumed_from < WINDOW_COUNT
    # The final value is emitted once, with the full aggregate.
    assert first + second == expected


def test_drain_with_snapshot_interval_pauses_window(entry_point, tmp_path):
    expected = _uninterrupted(
        entry_point, _window_flow, tmp_path / "reference", SNAPSHOT_TD
    )
    assert expected == [("ALL", (0, list(range(WINDOW_COUNT))))]

    first, second, resumed_from = _drained_then_resumed(
        entry_point, _window_flow, tmp_path / "drained", SNAPSHOT_TD
    )
    assert resumed_from is not None and 0 < resumed_from < WINDOW_COUNT
    # The window stays open until the resumed execution closes it.
    assert first == []
    assert first + second == expected


def test_drain_with_snapshot_interval_pauses_fold(entry_point, tmp_path):
    expected = _uninterrupted(
        entry_point, _fold_flow, tmp_path / "reference", SNAPSHOT_TD
    )
    assert expected == [("ALL", sum(range(WINDOW_COUNT)))]

    first, second, resumed_from = _drained_then_resumed(
        entry_point, _fold_flow, tmp_path / "drained", SNAPSHOT_TD
    )
    assert resumed_from is not None and 0 < resumed_from < WINDOW_COUNT
    assert first == []
    assert first + second == expected


class _DrainAtEndPartition(_SlowPartition):
    """Like `_SlowPartition`, but asks for a drain as its input ends.

    The natural end then arrives while the drain is pending.
    """

    def __init__(self, count, resume_state, requested):
        super().__init__(count, resume_state)
        self._requested = requested

    def next_batch(self):
        if self._next >= self._count:
            if not self._requested[0]:
                self._requested[0] = True
                bytewax_run.request_graceful_drain()
            raise StopIteration()
        return super().next_batch()


class _DrainAtEndSource(FixedPartitionedSource):
    def __init__(self, count, requested):
        self._count = count
        self._requested = requested

    def list_parts(self):
        return ["end"]

    def build_part(self, step_id, for_part, resume_state):
        return _DrainAtEndPartition(self._count, resume_state, self._requested)


def _drain_at_end_flow(sink, requested):
    flow = Dataflow("graceful_drain_natural_end")
    s = op.input("inp", flow, _DrainAtEndSource(WINDOW_COUNT, requested))
    s = op.key_on("key", s, lambda _item: "ALL")
    total = op.fold_final("total", s, int, lambda acc, item: acc + item)
    op.output("out", total, sink)
    return flow


def test_natural_end_during_drain_defers_final_output(entry_point, tmp_path):
    # The inputs reach their natural end while a drain is pending. The
    # fold's final output then waits for the next execution, as documented.
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    init_db_dir(store_dir, 1)
    recovery_config = RecoveryConfig(str(store_dir))
    requested = [False]

    first = []
    entry_point(
        _drain_at_end_flow(TestingSink(first), requested),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    assert requested == [True]
    assert first == []

    second = []
    entry_point(
        _drain_at_end_flow(TestingSink(second), requested),
        epoch_interval=EPOCH_TD,
        recovery_config=recovery_config,
    )
    assert second == [("ALL", sum(range(WINDOW_COUNT)))]


def _dynamic_flow(count, sink):
    """Like `_slow_flow`, but every worker reads a partition of its own.

    Each process then has input of its own, so draining one process
    cannot finish a cluster while another process keeps reading.
    """
    flow = Dataflow("graceful_drain_dynamic")
    s = op.input("inp", flow, _SlowDynamicSource(count))
    s = op.key_on("key", s, lambda _item: "ALL")
    s = op.stateful_map("sum", s, _running_total)
    op.output("out", s, sink)
    return flow


# Runs one `cluster_main` process as a driver. Process 0 and 1 of a
# multi-process cluster are started from here; an empty recovery dir
# means no recovery store.
_CLUSTER_DRIVER = """
import sys

tests_dir, recovery_dir, addresses, proc_id, out_path, count = sys.argv[1:]
sys.path.insert(0, tests_dir)

from bytewax.recovery import RecoveryConfig
from bytewax.testing import cluster_main
from test_graceful_drain import EPOCH_TD, _dynamic_flow, _LineSink

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


def _free_addresses(count):
    socks = [socket.socket() for _ in range(count)]
    try:
        for sock in socks:
            sock.bind(("127.0.0.1", 0))
        return [f"127.0.0.1:{sock.getsockname()[1]}" for sock in socks]
    finally:
        for sock in socks:
            sock.close()


@mark.parametrize("how", ["abort", "crash"])
def test_unclean_exit_resumes_on_next_run(tmp_path, how):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))
    out = []
    if how == "abort":
        source = TestingSource([*range(5), TestingSource.ABORT(), *range(5, 10)])
        flow = _plain_flow(source, out)
        run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    else:
        flow = _plain_flow(TestingSource(range(10)), out, _crash_once([False]))
        with raises(RuntimeError):
            run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)

    # The next execution finishes the input. A stateless flow may replay
    # items from before the failure, but none after it may be lost.
    out.clear()
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    assert set(range(5, 10)) <= set(out)


# Runs a `cluster_main` process with two workers and INFO tracing on, to
# check that the clean close is logged once per process, not per worker.
_WORKERS_LOG_CODE = """
import sys

tests_dir, recovery_dir = sys.argv[1], sys.argv[2]
sys.path.insert(0, tests_dir)

from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSink, cluster_main
from bytewax.tracing import setup_tracing
from test_graceful_drain import EPOCH_TD, _dynamic_flow

setup_tracing(log_level="INFO")
cluster_main(
    _dynamic_flow(20, TestingSink([])),
    [],
    0,
    worker_count_per_proc=2,
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir),
)
"""


@mark.skipif(IS_WINDOWS, reason="SIGTERM and SIGKILL are not available on Windows")
@mark.parametrize("how", ["drain", "kill9", "sigint"])
def test_clean_close_is_logged(tmp_path, how):
    # A drained execution ends normally and logs that it closed cleanly.
    # A killed or interrupted one never reaches that line.
    init_db_dir(tmp_path, 1)
    first_out = tmp_path / "first.txt"
    proc = _spawn(
        tmp_path,
        first_out,
        COUNT,
        drain_on_sigterm=how == "drain",
        log_info=True,
    )
    sig = {"drain": signal.SIGTERM, "kill9": signal.SIGKILL, "sigint": signal.SIGINT}
    try:
        _wait_for_items(proc, first_out, 3)
        proc.send_signal(sig[how])
        stdout, stderr = proc.communicate(timeout=60)
    finally:
        _kill_if_running(proc)

    if how == "drain":
        assert proc.returncode == 0, stderr.decode()
    closed = "Execution 0 closed cleanly" in stdout.decode()
    assert closed == (how == "drain"), stdout.decode()


# Runs an execution that aborts after two items, with INFO tracing on.
_ABORT_LOG_CODE = """
import sys

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.tracing import setup_tracing

setup_tracing(log_level="INFO")
flow = Dataflow("abort_log")
s = op.input("inp", flow, TestingSource([0, 1, TestingSource.ABORT(), 2]))
op.output("out", s, TestingSink([]))
run_main(flow, recovery_config=RecoveryConfig(sys.argv[1]))
"""


def test_aborted_execution_is_not_logged_as_clean(tmp_path):
    init_db_dir(tmp_path, 1)
    proc = subprocess.run(
        [sys.executable, "-c", _ABORT_LOG_CODE, str(tmp_path)],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    assert "closed cleanly" not in proc.stdout.decode(), proc.stdout.decode()


def test_clean_close_is_logged_once_per_process(tmp_path):
    init_db_dir(tmp_path, 2)
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            _WORKERS_LOG_CODE,
            str(Path(__file__).resolve().parent),
            str(tmp_path),
        ],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    stdout = proc.stdout.decode()
    assert stdout.count("Execution 0 closed cleanly") == 1, stdout


# Runs one process of a cluster with INFO tracing on, so each process's
# clean close shows up on its own stdout.
_CLUSTER_LOG_CODE = """
import sys

tests_dir, recovery_dir, addresses, proc_id = sys.argv[1:]
sys.path.insert(0, tests_dir)

from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSink, cluster_main
from bytewax.tracing import setup_tracing
from test_graceful_drain import EPOCH_TD, _dynamic_flow

setup_tracing(log_level="INFO")
cluster_main(
    _dynamic_flow(20, TestingSink([])),
    addresses.split(","),
    int(proc_id),
    epoch_interval=EPOCH_TD,
    recovery_config=RecoveryConfig(recovery_dir),
)
"""


def test_clean_close_is_logged_once_in_each_process(tmp_path):
    init_db_dir(tmp_path, 2)
    addresses = _free_addresses(2)
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _CLUSTER_LOG_CODE,
                str(Path(__file__).resolve().parent),
                str(tmp_path),
                ",".join(addresses),
                str(proc_id),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for proc_id in range(2)
    ]
    try:
        outputs = [proc.communicate(timeout=120) for proc in procs]
    finally:
        for proc in procs:
            _kill_if_running(proc)

    for proc, (stdout, stderr) in zip(procs, outputs):
        assert proc.returncode == 0, stderr.decode()
        assert stdout.decode().count("Execution 0 closed cleanly") == 1


# The recipe handler is installed first, so the first SIGTERM only requests
# a drain. The source is stuck in a call, so the drain cannot close the
# execution, and the handler has restored the default action for a second
# SIGTERM.
_STUCK_CODE = """
import signal
import sys
import time

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, run_main

tests_dir, recovery_dir = sys.argv[1], sys.argv[2]
sys.path.insert(0, tests_dir)

from test_graceful_drain import _drain_on_sigterm


def _handler(signum, frame):
    _drain_on_sigterm(signum, frame)
    # Tell the test the first SIGTERM was handled, so its second one
    # cannot coalesce with the first while both are pending.
    print("drain requested", flush=True)


signal.signal(signal.SIGTERM, _handler)


class _StuckPartition(StatelessSourcePartition):
    def __init__(self):
        self._sent = False

    def next_batch(self):
        if not self._sent:
            self._sent = True
            return [0]
        # A call that does not return: the drain cannot reach the input
        # until it does.
        print("stuck", flush=True)
        time.sleep(60)
        raise StopIteration

    def next_awake(self):
        return None


class _StuckSource(DynamicSource):
    def build(self, step_id, worker_index, worker_count):
        return _StuckPartition()


init_db_dir(recovery_dir, 1)
flow = Dataflow("stuck")
s = op.input("inp", flow, _StuckSource())
op.output("out", s, TestingSink([]))
run_main(flow, recovery_config=RecoveryConfig(recovery_dir))
"""


@mark.skipif(IS_WINDOWS, reason="SIGTERM is not catchable on Windows")
def test_second_sigterm_after_drain_handler_terminates(tmp_path):
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _STUCK_CODE,
            str(Path(__file__).resolve().parent),
            str(tmp_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout.readline().strip() == b"stuck"
        proc.send_signal(signal.SIGTERM)
        # Two pending SIGTERMs coalesce into one, so wait until the first
        # was handled before sending the second.
        assert proc.stdout.readline().strip() == b"drain requested"
        # The first SIGTERM only requested the drain, so the process runs on.
        assert proc.poll() is None
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            msg = "process still running 20 s after a second SIGTERM"
            raise AssertionError(msg) from None
    finally:
        _kill_if_running(proc)

    assert proc.returncode == -signal.SIGTERM


if __name__ == "__main__":
    recovery_dir, out_file, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
    if "--drain-on-sigterm" in sys.argv[4:]:
        signal.signal(signal.SIGTERM, _drain_on_sigterm)
    if "--log-info" in sys.argv[4:]:
        setup_tracing(log_level="INFO")
    driver_flow = _slow_flow(count, _LineSink(out_file))
    run_main(
        driver_flow,
        epoch_interval=EPOCH_TD,
        recovery_config=RecoveryConfig(recovery_dir),
    )
    print(f"emitted {len(_read_items(out_file))}")
