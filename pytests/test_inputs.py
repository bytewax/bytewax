import asyncio
import queue
from datetime import datetime, timedelta, timezone

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


def test_flow_requires_input():
    flow = Dataflow("test_df")

    with raises(ValueError):
        run_main(flow)


class TestPartition(StatelessSourcePartition):
    def __init__(self):
        self._next_awake = datetime.now(timezone.utc)
        self._iter = iter(list(range(5)))

    def next_batch(self):
        now = datetime.now(timezone.utc)
        # Assert that `next` is only called after
        # the `next_awake` time has passed.
        assert now >= self._next_awake
        # Request to awake in 0.1 seconds from now
        self._next_awake = now + timedelta(seconds=0.1)
        # This will raise StopIteration when the iterator is complete
        return [next(self._iter)]

    def next_awake(self):
        return self._next_awake


class TestSource(DynamicSource):
    def build(self, worker_index, worker_count):
        return TestPartition()


def test_next_awake_in_dynamic_input():
    """Test that the `next` method is not called before `next_awake` time."""

    out = []

    flow = Dataflow("test_df")
    s = flow.input("in", TestSource())
    s.output("out", TestingSink(out))

    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    assert out == [0, 1, 2, 3, 4]


class TestPartition(StatefulSourcePartition):
    def __init__(self, cooldown):
        self._cooldown = cooldown
        self._next_awake = datetime.now(timezone.utc)
        self._iter = iter(list(range(4)))

    def next_batch(self):
        now = datetime.now(timezone.utc)
        # Assert that `next` is only called after
        # the `next_awake` time has passed.
        assert now >= self._next_awake

        # Request to awake in self._cooldown seconds from now
        self._next_awake = now + self._cooldown
        # This will raise StopIteration when the iterator is complete
        return [next(self._iter)]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        pass


class TestSource(FixedPartitionedSource):
    def list_parts(self):
        return ["one", "two", "three"]

    def build_part(self, for_part, resume_state):
        if for_part == "one":
            return TestPartition(cooldown=timedelta(seconds=0.1))
        if for_part == "two":
            return TestPartition(cooldown=timedelta(seconds=0.2))
        if for_part == "three":
            return TestPartition(cooldown=timedelta(seconds=0.3))


def test_next_awake_in_partitioned_input():
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestSource())
    s.output("out", TestingSink(out))

    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    assert out == [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]


def test_simple_polling_source_align_to():
    part = _SimplePollingPartition(
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
        now_getter=lambda: datetime(2023, 1, 1, 5, 15, tzinfo=timezone.utc),
    )
    assert part.next_awake() == datetime(2023, 1, 1, 5, 30, tzinfo=timezone.utc)


def test_simple_polling_source_align_to_start_on_align_awakes_immediately():
    part = _SimplePollingPartition(
        interval=timedelta(minutes=30),
        align_to=datetime(2023, 1, 1, 4, 0, tzinfo=timezone.utc),
        getter=lambda: True,
        now_getter=lambda: datetime(2023, 1, 1, 5, 0, tzinfo=timezone.utc),
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
