from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicInput,
    StatelessSource,
    PartitionedInput,
    StatefulSource,
)
from bytewax.testing import run_main, TestingOutput


def test_next_awake_in_dynamic_input():
    """Test that the `next` method is not called before `next_awake` time."""

    class TestSource(StatelessSource):
        def __init__(self):
            self._next_awake = datetime.now(timezone.utc)
            self._iter = iter(list(range(5)))

        def next(self):
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

    class TestInput(DynamicInput):
        def build(self, worker_index, worker_count):
            return TestSource()

    out = []
    flow = Dataflow()
    flow.input("in", TestInput())
    flow.output("out", TestingOutput(out))
    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    expected = [0, 1, 2, 3, 4]
    assert out == expected


def test_next_awake_in_partitioned_input():
    """Test that the `next` method is not called before `next_awake` time."""

    class TestSource(StatefulSource):
        def __init__(self, cooldown):
            self._cooldown = cooldown
            self._next_awake = datetime.now(timezone.utc)
            self._iter = iter(list(range(4)))

        def next(self):
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

    class TestInput(PartitionedInput):
        def list_parts(self):
            return {"one", "two", "three"}

        def build_part(self, for_part, resume_state):
            if for_part == "one":
                return TestSource(cooldown=timedelta(seconds=0.1))
            if for_part == "two":
                return TestSource(cooldown=timedelta(seconds=0.2))
            if for_part == "three":
                return TestSource(cooldown=timedelta(seconds=0.3))

    out = []
    flow = Dataflow()
    flow.input("in", TestInput())
    flow.output("out", TestingOutput(out))
    # If next is called before the next_awake time has passed,
    # the dataflow will crash and the test won't pass.
    run_main(flow)
    expected = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert sorted(out) == expected
