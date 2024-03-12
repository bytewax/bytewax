import asyncio
import itertools
import queue
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import (
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
from bytewax.testing import TestingSink, run_main
from pytest import raises
from typing_extensions import override


def test_flow_requires_input():
    flow = Dataflow("test_df")

    expect = "at least one input"
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
        assert td >= interval


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
        assert td >= fast_interval


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
        assert td >= interval


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
        assert td >= fast_interval


def test_simple_polling_source_interval():
    now = datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc)

    part = _SimplePollingPartition(
        now,
        interval=timedelta(minutes=30),
        align_to=now,
        getter=lambda: True,
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
    )
    assert part.next_batch() == []
    assert part.next_awake() == datetime(2023, 1, 1, 5, 0, 5, tzinfo=timezone.utc)


def test_simple_polling_source_align_to():
    part = _SimplePollingPartition(
        datetime(2023, 1, 1, 5, 15, tzinfo=timezone.utc),
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
    )
    assert part.next_awake() == datetime(2023, 1, 1, 5, 30, tzinfo=timezone.utc)


def test_simple_polling_source_align_to_start_on_align_awakes_immediately():
    part = _SimplePollingPartition(
        datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
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
