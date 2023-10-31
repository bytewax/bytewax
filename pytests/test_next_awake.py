import itertools
from datetime import datetime, timedelta, timezone
from typing import Any, List

from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    FixedPartitionedSource,
    StatefulSourcePartition,
    StatelessSourcePartition,
)
from bytewax.testing import TestingSink, run_main


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
