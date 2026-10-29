import asyncio
import gc
import itertools
import os
import queue
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    AbortExecution,
    DynamicSource,
    FixedPartitionedSource,
    SimplePollingSource,
    StatefulSourcePartition,
    StatelessSourcePartition,
    _SimplePollingPartition,
    batch,
    batch_async,
    batch_getter,
    batch_getter_ex,
)
from bytewax.testing import (
    TestingSink,
    TestingSource,
    _IterSourcePartition,
    cluster_main,
    run_main,
)
from pytest import raises, skip
from typing_extensions import override


def test_flow_requires_input():
    flow = Dataflow("test_df")

    expect = "at least one input"
    with raises(RuntimeError):
        with raises(ValueError, match=re.escape(expect)):
            run_main(flow)


def test_dynamic_source_next_batch_iterator():
    out = []

    class TestPartition(StatelessSourcePartition[int]):
        def __init__(self):
            self._n = 0

        @override
        def next_batch(self) -> Iterable[int]:
            if self._n < 5:
                n = self._n
                self._n += 1
                return itertools.repeat(n, 2)
            else:
                raise StopIteration()

    class TestSource(DynamicSource[int]):
        @override
        def build(
            self, _step_id: str, _worker_index: int, _worker_count: int
        ) -> TestPartition:
            return TestPartition()

    flow = Dataflow("test_df")
    s = op.input("in", flow, TestSource())
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]


def test_fixed_partitioned_source_next_batch_iterator():
    out = []

    class TestPartition(StatefulSourcePartition[int, None]):
        def __init__(self):
            self._n = 0

        @override
        def next_batch(self) -> Iterable[int]:
            if self._n < 5:
                n = self._n
                self._n += 1
                return itertools.repeat(n, 2)
            else:
                raise StopIteration()

        @override
        def snapshot(self) -> None:
            return None

    class TestSource(FixedPartitionedSource[int, None]):
        @override
        def list_parts(self) -> List[str]:
            return ["one"]

        @override
        def build_part(
            self, step_id: str, for_part: str, resume_state: None
        ) -> TestPartition:
            return TestPartition()

    flow = Dataflow("test_df")
    s = op.input("in", flow, TestSource())
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]


def pairwise(ib):
    # Recipe from
    # https://docs.python.org/3/library/itertools.html?highlight=pairwise#itertools.pairwise
    a, b = itertools.tee(ib)
    next(b, None)
    return zip(a, b)


_CLOCK_TOLERANCE = timedelta(milliseconds=1) if sys.platform == "win32" else timedelta()


def assert_interval_elapsed(td, interval):
    assert td + _CLOCK_TOLERANCE >= interval


class _DynamicMetronomePartition(StatelessSourcePartition[Tuple[datetime, int]]):
    def __init__(self, interval: timedelta, count: int, next_awake: datetime, n: int):
        self._interval = interval
        self._count = count
        self._next_awake = next_awake
        self._n = n

    @override
    def next_batch(self) -> List[Tuple[datetime, int]]:
        now = datetime.now(timezone.utc)
        self._next_awake = now + self._interval
        if self._n < 5:
            n = self._n
            self._n += 1
            return [(now, n)]
        else:
            raise StopIteration()

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake


class DynamicMetronomeSource(DynamicSource[Tuple[datetime, int]]):
    def __init__(self, interval: timedelta, count: int = sys.maxsize):
        self._interval = interval
        self._count = count

    @override
    def build(
        self, _step_id: str, worker_index: int, worker_count: int
    ) -> _DynamicMetronomePartition:
        now = datetime.now(timezone.utc)
        return _DynamicMetronomePartition(self._interval, self._count, now, 0)


def test_dynamic_source_next_awake():
    out = []

    interval = timedelta(seconds=0.1)

    flow = Dataflow("test_df")
    s = op.input("in", flow, DynamicMetronomeSource(interval))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    for x, y in pairwise(out):
        x_time, _ = x
        y_time, _ = y
        td = y_time - x_time
        assert_interval_elapsed(td, interval)


def test_dynamic_source_advances_epoch_even_if_not_awoken():
    fast_out = []
    slow_out = []

    fast_interval = timedelta(seconds=0.1)
    slow_interval = timedelta(seconds=0.5)

    flow = Dataflow("test_df")
    fast_s = op.input("fast_inp", flow, DynamicMetronomeSource(fast_interval, 5))
    op.output("fast_out", fast_s, TestingSink(fast_out))
    slow_s = op.input("slow_inp", flow, DynamicMetronomeSource(slow_interval, 5))
    op.output("slow_out", slow_s, TestingSink(slow_out))

    run_main(flow, epoch_interval=timedelta(seconds=0.25))
    for x, y in pairwise(fast_out):
        x_time, _ = x
        y_time, _ = y
        td = y_time - x_time
        assert_interval_elapsed(td, fast_interval)


class _MetronomePartition(
    StatefulSourcePartition[Tuple[datetime, int], Tuple[datetime, int]]
):
    def __init__(self, interval: timedelta, count: int, next_awake: datetime, n: int):
        self._interval = interval
        self._count = count
        self._next_awake = next_awake
        self._n = n

    @override
    def next_batch(self) -> Iterable[Tuple[datetime, int]]:
        now = datetime.now(timezone.utc)
        self._next_awake = now + self._interval
        if self._n < self._count:
            n = self._n
            self._n += 1
            return [(now, n)]
        else:
            raise StopIteration()

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

    @override
    def snapshot(self) -> Tuple[datetime, int]:
        return (self._next_awake, self._n)


class MetronomeSource(
    FixedPartitionedSource[Tuple[datetime, int], Tuple[datetime, int]]
):
    def __init__(self, interval: timedelta, count: int = sys.maxsize):
        self._interval = interval
        self._count = count

    @override
    def list_parts(self) -> List[str]:
        return ["singleton"]

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[Tuple[datetime, int]]
    ) -> _MetronomePartition:
        if resume_state is not None:
            next_awake, n = resume_state
        else:
            next_awake = datetime.now(timezone.utc)
            n = 0
        return _MetronomePartition(self._interval, self._count, next_awake, n)


def test_fixed_partitioned_source_next_awake():
    out = []

    interval = timedelta(seconds=0.1)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, MetronomeSource(interval, 5))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    for x, y in pairwise(out):
        x_time, _ = x
        y_time, _ = y
        td = y_time - x_time
        assert_interval_elapsed(td, interval)


def test_fixed_partitioned_source_advances_epoch_even_if_not_awoken():
    fast_out = []
    slow_out = []

    fast_interval = timedelta(seconds=0.1)
    slow_interval = timedelta(seconds=0.5)

    flow = Dataflow("test_df")
    fast_s = op.input("fast_inp", flow, MetronomeSource(fast_interval, 5))
    op.output("fast_out", fast_s, TestingSink(fast_out))
    slow_s = op.input("slow_inp", flow, MetronomeSource(slow_interval, 5))
    op.output("slow_out", slow_s, TestingSink(slow_out))

    run_main(flow, epoch_interval=timedelta(seconds=0.25))
    for x, y in pairwise(fast_out):
        x_time, _ = x
        y_time, _ = y
        td = y_time - x_time
        assert_interval_elapsed(td, fast_interval)


class SimpleListSource(SimplePollingSource[str, int]):
    @dataclass
    class Abort:
        _triggered: bool = False

    def __init__(self, items: List[str]) -> None:
        self.items = items
        self._next_idx = 0

        super().__init__(interval=timedelta(seconds=0))

    @override
    def next_item(self) -> str:
        try:
            item = self.items[self._next_idx]
            self._next_idx += 1

            if isinstance(item, SimpleListSource.Abort) and not item._triggered:
                item._triggered = True
                raise AbortExecution()

            return item
        except IndexError as ex:
            raise StopIteration() from ex

    @override
    def snapshot(self) -> int:
        return self._next_idx

    @override
    def resume(self, resume_state: int) -> None:
        self._next_idx = resume_state


def test_simple_polling_source_resume_state():
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp", flow, SimpleListSource(["a", "b", SimpleListSource.Abort(), "c"])
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == ["a", "b"]

    out.clear()
    run_main(flow)
    assert out == ["c"]


def test_simple_polling_source_interval():
    now = datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc)

    part = _SimplePollingPartition(
        now,
        interval=timedelta(minutes=30),
        align_to=now,
        getter=lambda: True,
        snapshot=lambda: None,
    )
    assert part.next_batch() == [True]
    assert part.next_awake() == datetime(2023, 1, 1, 5, 30, tzinfo=timezone.utc)


def test_simple_polling_source_retry():
    now = datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc)

    def getter():
        raise SimplePollingSource.Retry(timedelta(seconds=5))

    part = _SimplePollingPartition(
        now,
        interval=timedelta(minutes=30),
        align_to=now,
        getter=getter,
        snapshot=lambda: None,
    )
    assert part.next_batch() == []
    assert part.next_awake() == datetime(2023, 1, 1, 5, 0, 5, tzinfo=timezone.utc)


def test_simple_polling_source_align_to():
    part = _SimplePollingPartition(
        datetime(2023, 1, 1, 5, 15, tzinfo=timezone.utc),
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
        snapshot=lambda: None,
    )
    assert part.next_awake() == datetime(2023, 1, 1, 5, 30, tzinfo=timezone.utc)


def test_simple_polling_source_align_to_start_on_align_awakes_immediately():
    part = _SimplePollingPartition(
        datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
        snapshot=lambda: None,
    )
    assert part.next_awake() == datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc)


def test_batch():
    batcher = batch(range(5), 3)
    assert next(batcher) == [0, 1, 2]
    assert next(batcher) == [3, 4]
    with raises(StopIteration):
        next(batcher)
    with raises(StopIteration):
        next(batcher)


class CloseableQueue:
    def __init__(self):
        self.q = []
        self.closed = False

    def put(self, x):
        assert not self.closed
        self.q.append(x)

    def get(self):
        try:
            return self.q.pop(0)
        except IndexError:
            if not self.closed:
                raise queue.Empty() from None
            else:
                raise StopIteration() from None

    def close(self):
        self.closed = True


def test_batch_getter():
    q = CloseableQueue()

    def getter():
        try:
            return q.get()
        except queue.Empty:
            return None

    batcher = batch_getter(getter, 3)
    q.put(0)
    q.put(1)
    q.put(2)
    q.put(3)
    q.put(4)
    assert next(batcher) == [0, 1, 2]
    assert next(batcher) == [3, 4]
    assert next(batcher) == []
    q.put(5)
    q.close()
    assert next(batcher) == [5]
    with raises(StopIteration):
        next(batcher)
    with raises(StopIteration):
        next(batcher)


def test_batch_getter_ex():
    q = CloseableQueue()
    batcher = batch_getter_ex(q.get, 3)
    q.put(0)
    q.put(1)
    q.put(2)
    q.put(3)
    q.put(4)
    assert next(batcher) == [0, 1, 2]
    assert next(batcher) == [3, 4]
    assert next(batcher) == []
    q.put(5)
    q.close()
    assert next(batcher) == [5]
    with raises(StopIteration):
        next(batcher)
    with raises(StopIteration):
        next(batcher)


async def _gen():
    for i in range(5):
        await asyncio.sleep(0)
        yield i


def test_batch_async():
    batcher = batch_async(_gen(), timeout=timedelta(seconds=1), batch_size=2)
    assert next(batcher) == [0, 1]
    assert next(batcher) == [2, 3]
    assert next(batcher) == [4]
    with raises(StopIteration):
        next(batcher)
    with raises(StopIteration):
        next(batcher)


class _LoopRecorder:
    """Record the event loops `batch_async` creates for itself."""

    def __init__(self, monkeypatch):
        self.loops: List[asyncio.AbstractEventLoop] = []
        orig = asyncio.new_event_loop

        def new_event_loop():
            loop = orig()
            self.loops.append(loop)
            return loop

        monkeypatch.setattr(asyncio, "new_event_loop", new_event_loop)


def test_batch_async_closes_own_loop_when_exhausted(monkeypatch):
    rec = _LoopRecorder(monkeypatch)

    assert list(batch_async(_gen(), timedelta(seconds=1), 2)) == [[0, 1], [2, 3], [4]]

    (loop,) = rec.loops
    assert loop.is_closed()


def test_batch_async_closes_own_loop_and_source_when_abandoned(monkeypatch):
    rec = _LoopRecorder(monkeypatch)
    cleaned_up = []

    async def endless():
        try:
            i = 0
            while True:
                await asyncio.sleep(0)
                yield i
                i += 1
        finally:
            # E.g. closing a websocket.
            cleaned_up.append(True)

    batcher = batch_async(endless(), timedelta(seconds=1), 2)
    assert next(batcher) == [0, 1]
    # A partition being closed mid-stream drops its batcher.
    batcher.close()

    (loop,) = rec.loops
    assert loop.is_closed()
    assert cleaned_up == [True]


def test_batch_async_leaves_given_loop_open():
    loop = asyncio.new_event_loop()
    try:
        batcher = batch_async(_gen(), timedelta(seconds=1), 5, loop=loop)
        assert list(batcher) == [[0, 1, 2, 3, 4]]

        assert not loop.is_closed()
    finally:
        loop.close()


def _open_fds() -> int:
    for fd_dir in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(fd_dir):
            return len(os.listdir(fd_dir))
    skip("can't list open file descriptors on this platform")


def test_batch_async_does_not_leak_fds():
    # Each event loop holds file descriptors (a selector, and a
    # self-pipe or socket pair). Without GC, a loop that isn't closed
    # keeps them open, so a leak shows up as the count growing.
    gc.collect()
    gc.disable()
    try:
        before = _open_fds()
        for _ in range(20):
            assert list(batch_async(_gen(), timedelta(seconds=1), 5)) == [
                [0, 1, 2, 3, 4]
            ]
        after = _open_fds()
    finally:
        gc.enable()
        gc.collect()

    assert after - before < 20


class _SleepyPartition(StatefulSourcePartition[str, None]):
    def __init__(self, key: str, delay: float, until: float) -> None:
        self._key = key
        self._delay = delay
        self._until = until

    @override
    def next_batch(self) -> List[str]:
        if time.monotonic() >= self._until:
            raise StopIteration()
        time.sleep(self._delay)
        return [self._key]

    @override
    def snapshot(self) -> None:
        return None


class _SleepySource(FixedPartitionedSource[str, None]):
    """Each partition emits its own key after sleeping for its delay."""

    def __init__(self, delays: Dict[str, float], run_for: timedelta) -> None:
        self._delays = delays
        self._run_for = run_for
        self._until: Optional[float] = None

    @override
    def list_parts(self) -> List[str]:
        return list(self._delays)

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[None]
    ) -> _SleepyPartition:
        if self._until is None:
            self._until = time.monotonic() + self._run_for.total_seconds()
        return _SleepyPartition(for_part, self._delays[for_part], self._until)


def assert_epochs_not_starved(per_epoch: Counter) -> None:
    """Assert every epoch but the last holds about one epoch of batches.

    The last epoch is cut short by the end of the run. Any epoch the
    partition spent waiting on the probe would otherwise hold only a
    batch or two.
    """
    epochs = sorted(per_epoch)
    full = {epoch: per_epoch[epoch] for epoch in epochs[:-1]}
    assert full, f"too few epochs to judge: {dict(per_epoch)}"
    # Each full epoch is 1 s / 30 ms ~ 33 batches; 5x slower is ~7.
    assert min(full.values()) >= 3, (
        f"an epoch held only {min(full.values())} batches (want >= 3) "
        f"after waiting on the probe; per epoch: {dict(sorted(per_epoch.items()))}"
    )


class _StallOncePartition(StatefulSourcePartition[str, None]):
    """Sleep `stall` seconds on the first batch, emit one item, then end."""

    def __init__(self, key: str, stall: float) -> None:
        self._key = key
        self._stall = stall
        self._done = False

    @override
    def next_batch(self) -> List[str]:
        if self._done:
            raise StopIteration()
        self._done = True
        time.sleep(self._stall)
        return [self._key]

    @override
    def snapshot(self) -> None:
        return None


class _StallingSource(FixedPartitionedSource[str, None]):
    """Partition `fast` sleeps `fast_delay` per batch; `stall` stalls once."""

    def __init__(self, fast_delay: float, stall: float, run_for: timedelta) -> None:
        self._fast_delay = fast_delay
        self._stall = stall
        self._run_for = run_for
        self._until: Optional[float] = None

    @override
    def list_parts(self) -> List[str]:
        return ["fast", "stall"]

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[None]
    ) -> StatefulSourcePartition[str, None]:
        if self._until is None:
            self._until = time.monotonic() + self._run_for.total_seconds()
        if for_part == "stall":
            return _StallOncePartition(for_part, self._stall)
        return _SleepyPartition(for_part, self._fast_delay, self._until)


def test_fixed_partitioned_source_partition_alone_on_worker_keeps_up():
    # `fast` and `stall` run on different workers. `stall` takes three
    # seconds, three epochs, to produce its first item, so the frontier
    # stays behind `fast` and `fast` runs ahead and then waits on the
    # probe for `stall`. That wait must not count as time spent in the
    # epoch it blocks: once `stall` finishes, `fast` should get a whole
    # epoch of batches back. Otherwise the wait alone exceeds the epoch
    # interval and that epoch holds a batch or two. The stall and the
    # interval are long compared to scheduling jitter, so the outcome
    # is set by the mechanism, not by machine speed.
    out: List[Tuple[str, int, int]] = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        _StallingSource(fast_delay=0.03, stall=3.0, run_for=timedelta(seconds=5)),
    )
    op.inspect_debug(
        "record",
        s,
        lambda _step_id, key, epoch, worker: out.append((key, epoch, worker)),
    )

    cluster_main(
        flow, [], 0, epoch_interval=timedelta(seconds=1), worker_count_per_proc=2
    )

    workers = {key: worker for key, _, worker in out}
    assert workers["fast"] != workers["stall"], "partitions must not share a worker"
    fast_per_epoch = Counter(epoch for key, epoch, _ in out if key == "fast")
    assert_epochs_not_starved(fast_per_epoch)


def test_fixed_partitioned_source_waiting_does_not_busy_wait():
    # While `b` waits for the rest of the dataflow to finish the epoch,
    # it must not busy-wait. Before the cooldown this ratio was ~0.45.
    out: List[str] = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        _SleepySource({"a": 0.18, "b": 0.03, "c": 0.18}, timedelta(seconds=2)),
    )
    op.output("out", s, TestingSink(out))

    before_cpu = time.process_time()
    before_wall = time.monotonic()
    cluster_main(
        flow, [], 0, epoch_interval=timedelta(seconds=0.5), worker_count_per_proc=2
    )
    cpu = time.process_time() - before_cpu
    wall = time.monotonic() - before_wall

    ratio = cpu / wall
    assert ratio < 0.15, (
        f"busy-waiting partition: {cpu:.2f} CPU seconds over {wall:.2f} "
        f"wall seconds (ratio {ratio:.2f}, want < 0.15)"
    )


class _SleepyStatelessPartition(StatelessSourcePartition[int]):
    def __init__(self, worker_index: int, delay: float, until: float) -> None:
        self._worker_index = worker_index
        self._delay = delay
        self._until = until

    @override
    def next_batch(self) -> List[int]:
        if time.monotonic() >= self._until:
            raise StopIteration()
        time.sleep(self._delay)
        return [self._worker_index]


class _StallStatelessPartition(StatelessSourcePartition[int]):
    """Sleep `stall` seconds on the first batch, emit one item, then end."""

    def __init__(self, worker_index: int, stall: float) -> None:
        self._worker_index = worker_index
        self._stall = stall
        self._done = False

    @override
    def next_batch(self) -> List[int]:
        if self._done:
            raise StopIteration()
        self._done = True
        time.sleep(self._stall)
        return [self._worker_index]


class _StallingDynamicSource(DynamicSource[int]):
    """One worker stalls once; every other worker emits every 30 ms."""

    def __init__(self, stall_worker: int, stall: float, run_for: timedelta) -> None:
        self._stall_worker = stall_worker
        self._stall = stall
        self._until = time.monotonic() + run_for.total_seconds()

    @override
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSourcePartition[int]:
        if worker_index == self._stall_worker:
            return _StallStatelessPartition(worker_index, self._stall)
        return _SleepyStatelessPartition(worker_index, 0.03, self._until)


class _SleepyDynamicSource(DynamicSource[int]):
    """Each worker emits its index after sleeping for its delay."""

    def __init__(self, delays: List[float], run_for: timedelta) -> None:
        self._delays = delays
        self._until = time.monotonic() + run_for.total_seconds()

    @override
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _SleepyStatelessPartition:
        return _SleepyStatelessPartition(
            worker_index, self._delays[worker_index], self._until
        )


def test_dynamic_source_fast_worker_keeps_up():
    # Worker 0 is fast and worker 1 stalls for three seconds before its
    # first item. Worker 0 waits on the probe for worker 1; that wait
    # must not count as time spent in the epoch it blocks. See the
    # fixed partitioned test above.
    out: List[Tuple[int, int, int]] = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        _StallingDynamicSource(stall_worker=1, stall=3.0, run_for=timedelta(seconds=5)),
    )
    op.inspect_debug(
        "record",
        s,
        lambda _step_id, item, epoch, worker: out.append((item, epoch, worker)),
    )

    cluster_main(
        flow, [], 0, epoch_interval=timedelta(seconds=1), worker_count_per_proc=2
    )

    fast_per_epoch = Counter(epoch for item, epoch, _ in out if item == 0)
    assert_epochs_not_starved(fast_per_epoch)


def test_dynamic_source_waiting_does_not_busy_wait():
    # While worker 0 waits for worker 1 to finish the epoch, it must not
    # busy-wait.
    out: List[int] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, _SleepyDynamicSource([0.03, 0.18], timedelta(seconds=2)))
    op.output("out", s, TestingSink(out))

    before_cpu = time.process_time()
    before_wall = time.monotonic()
    cluster_main(
        flow, [], 0, epoch_interval=timedelta(seconds=0.5), worker_count_per_proc=2
    )
    cpu = time.process_time() - before_cpu
    wall = time.monotonic() - before_wall

    ratio = cpu / wall
    assert ratio < 0.15, (
        f"busy-waiting worker: {cpu:.2f} CPU seconds over {wall:.2f} "
        f"wall seconds (ratio {ratio:.2f}, want < 0.15)"
    )


def test_fixed_partitioned_source_zero_epoch_interval_is_fast():
    # With `epoch_interval=0` and `batch_size=1` every item is its own
    # epoch, so the partition is blocked on the probe right after each
    # advance, and the probe needs up to one worker step per downstream
    # operator to advance. All re-checks within the cooldown grace must
    # be immediate: in a release build, backing off on every blocked
    # activation made 3000 items take 3.9 s here, and backing off from
    # the second blocked activation still took 1.3 ms per item in this
    # multi-operator pipeline, versus 0.016 s with the grace.
    out: List[int] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(range(3000), 1))
    s = op.map("ident_a", s, lambda x: x)
    s = op.map("ident_b", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    before = time.monotonic()
    run_main(flow, epoch_interval=timedelta(0))
    elapsed = time.monotonic() - before

    assert sorted(out) == list(range(3000))
    assert elapsed < 1.5, f"zero epoch interval took {elapsed:.2f} s (want < 1.5 s)"


class _CountingStatelessPartition(StatelessSourcePartition[int]):
    def __init__(self, count: int) -> None:
        self._items = iter(range(count))

    @override
    def next_batch(self) -> List[int]:
        return [next(self._items)]


class _CountingDynamicSource(DynamicSource[int]):
    """Emit one item per batch from each worker."""

    def __init__(self, count: int) -> None:
        self._count = count

    @override
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _CountingStatelessPartition:
        return _CountingStatelessPartition(self._count)


def test_dynamic_source_zero_epoch_interval_is_fast():
    # Same as the fixed partitioned case above, for `DynamicSource`.
    out: List[int] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, _CountingDynamicSource(3000))
    s = op.map("ident_a", s, lambda x: x)
    s = op.map("ident_b", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    before = time.monotonic()
    run_main(flow, epoch_interval=timedelta(0))
    elapsed = time.monotonic() - before

    assert sorted(out) == list(range(3000))
    assert elapsed < 1.5, f"zero epoch interval took {elapsed:.2f} s (want < 1.5 s)"


class _SlowStartSource(TestingSource[str]):
    """A `TestingSource` whose partition takes 5 ms to build."""

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _IterSourcePartition[str]:
        time.sleep(0.005)
        return super().build_part(step_id, for_part, resume_state)


def test_inputs_start_together():
    # At the start of an execution every input waits for the probe to
    # move past the first epoch, which takes several worker steps in
    # this dataflow. `first` starts waiting, then takes 5 ms, longer
    # than the cooldown grace, to build its partition; `second` starts
    # waiting only after that. If `first` cooled down on that wait, it
    # would sleep while `second` emitted all of its items.
    out: List[Tuple[str, str]] = []

    flow = Dataflow("test_df")
    first = op.input("first", flow, _SlowStartSource(["f1", "f2", "f3"]))
    second = op.input("second", flow, TestingSource(["s1", "s2", "s3"]))
    merged = op.merge("merge", first, second)
    keyed = op.key_on("key", merged, lambda _item: "all")
    passed = op.stateful_map("pass", keyed, lambda state, item: (state, item))
    op.output("out", passed, TestingSink(out))

    run_main(flow)

    items = [item for _key, item in out]
    assert items.index("f1") < items.index("s3"), f"first input started late: {items}"
