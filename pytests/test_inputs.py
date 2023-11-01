import asyncio
import itertools
import queue
from datetime import datetime, timedelta, timezone
from typing import Any, List

from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    FixedPartitionedSource,
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


def pairwise(ib):
    # Recipe from
    # https://docs.python.org/3/library/itertools.html?highlight=pairwise#itertools.pairwise
    a, b = itertools.tee(ib)
    next(b, None)
    return zip(a, b)


def test_dynamic_source_next_awake():
    out = []

    class TestPartition(StatelessSourcePartition):
        def __init__(self, now, interval):
            self._interval = interval
            self._next_awake = now
            self._n = 0

        def next_batch(self, _sched: datetime) -> List[Any]:
            now = datetime.now(timezone.utc)
            self._next_awake = now + self._interval
            if self._n < 5:
                self._n += 1
                return [now]
            else:
                raise StopIteration()

        def next_awake(self):
            return self._next_awake

    class TestSource(DynamicSource):
        def __init__(self, interval):
            self._interval = interval

        def build(self, now, _worker_index, _worker_count):
            return TestPartition(now, self._interval)

    interval = timedelta(seconds=0.1)

    flow = Dataflow()
    flow.input("in", TestSource(interval))
    flow.output("out", TestingSink(out))

    run_main(flow)
    for x, y in pairwise(out):
        td = y - x
        assert td >= interval


def test_fixed_partitioned_source_next_awake():
    out = []

    class TestPartition(StatefulSourcePartition):
        def __init__(self, now, interval):
            self._interval = interval
            self._next_awake = now
            self._n = 0

        def next_batch(self, _sched: datetime) -> List[Any]:
            now = datetime.now(timezone.utc)
            self._next_awake = now + self._interval
            if self._n < 5:
                self._n += 1
                return [now]
            else:
                raise StopIteration()

        def next_awake(self):
            return self._next_awake

        def snapshot(self):
            return None

    class TestSource(FixedPartitionedSource):
        def __init__(self, interval):
            self._interval = interval

        def list_parts(self):
            return ["one"]

        def build_part(self, now, _for_part, _resume_state):
            return TestPartition(now, self._interval)

    interval = timedelta(seconds=0.1)

    flow = Dataflow()
    flow.input("inp", TestSource(interval))
    flow.output("out", TestingSink(out))

    run_main(flow)
    for x, y in pairwise(out):
        td = y - x
        assert td >= interval


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
