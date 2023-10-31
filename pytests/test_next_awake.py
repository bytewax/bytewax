from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    FixedPartitionedSource,
    StatefulSourcePartition,
    StatelessSourcePartition,
)
from bytewax.testing import TestingSink, run_main


def test_next_awake_in_dynamic_input():
    """Test that the `next` method is not called before `next_awake` time."""

    class TestPartition(StatelessSourcePartition):
        def __init__(self, now):
            self._next_awake = now
            self._iter = iter(list(range(5)))

        def next_batch(self, sched):
            datetime.now(timezone.utc)
            # Assert that `next` is only called after
            # the `next_awake` time has passed.
            assert sched >= self._next_awake
            # Request to awake in 0.1 seconds from now
            self._next_awake = sched + timedelta(seconds=0.1)
            # This will raise StopIteration when the iterator is complete
            return [next(self._iter)]

        def next_awake(self):
            return self._next_awake

    class TestSource(DynamicSource):
        def build(self, now, worker_index, worker_count):
            return TestPartition(now)

    out = []
    flow = Dataflow()
    flow.input("in", TestSource())
    flow.output("out", TestingSink(out))
    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    expected = [0, 1, 2, 3, 4]
    assert out == expected


def test_next_awake_in_partitioned_input():
    """Test that the `next` method is not called before `next_awake` time."""

    class TestPartition(StatefulSourcePartition):
        def __init__(self, now, cooldown):
            self._cooldown = cooldown
            self._next_awake = now
            self._iter = iter(list(range(4)))

        def next_batch(self, sched):
            now = datetime.now(timezone.utc)
            # Assert that `next` is only called after
            # the `next_awake` time has passed.
            assert now >= self._next_awake

            # Request to awake in self._cooldown seconds from now
            self._next_awake = sched + self._cooldown
            # This will raise StopIteration when the iterator is complete
            return [next(self._iter)]

        def next_awake(self):
            return self._next_awake

        def snapshot(self):
            pass

    class TestSource(FixedPartitionedSource):
        def list_parts(self):
            return ["one", "two", "three"]

        def build_part(self, now, for_part, resume_state):
            if for_part == "one":
                return TestPartition(now, cooldown=timedelta(seconds=0.1))
            if for_part == "two":
                return TestPartition(now, cooldown=timedelta(seconds=0.2))
            if for_part == "three":
                return TestPartition(now, cooldown=timedelta(seconds=0.3))

    out = []
    flow = Dataflow()
    flow.input("in", TestSource())
    flow.output("out", TestingSink(out))
    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    expected = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert sorted(out) == expected
