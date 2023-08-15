"""Helper tools for testing dataflows.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.outputs import DynamicOutput, StatelessSink

from .bytewax import cluster_main, run_main

__all__ = [
    "run_main",
    "cluster_main",
    "poll_next_batch",
    "TestingInput",
    "TestingOutput",
]


class _IterSource(StatefulSource):
    def __init__(self, it, resume_state):
        self._last_idx = -1 if resume_state is None else resume_state
        self._it = enumerate(it)
        # Resume to one after the last completed read index.
        for i in range(self._last_idx + 1):
            next(self._it)

    def next_batch(self):
        # next will raise StopIteration on its own.
        self._last_idx, item = next(self._it)
        return [item]

    def snapshot(self):
        return self._last_idx


class TestingInput(PartitionedInput):
    """Produce input from a Python iterable. You only want to use this
    for unit testing.

    The iterable must be identical on all workers; this will
    automatically distribute the items across workers and handle
    recovery.

    Be careful using a generator as the iterable; if you fail and
    attempt to resume the dataflow without rebuilding it, the
    half-consumed generator will be re-used on recovery and early
    input will be lost so resume will see the correct data.

    Args:

        it: Iterable for input.

    """

    __test__ = False

    def __init__(self, it: Iterable[Any]):
        self._it = it

    def list_parts(self):
        return ["iter"]

    def build_part(self, for_key, resume_state):
        assert for_key == "iter"
        return _IterSource(self._it, resume_state)


class _ListSink(StatelessSink):
    def __init__(self, ls):
        self._ls = ls

    def write_batch(self, items):
        self._ls += items


class TestingOutput(DynamicOutput):
    """Append each output item to a list. You only want to use this
    for unit testing.

    Can support at-least-once processing. The list is not cleared
    between executions.

    Args:

        ls: List to append to.

    """

    __test__ = False

    def __init__(self, ls):
        self._ls = ls

    def build(self, worker_index, worker_count):
        return _ListSink(self._ls)


def poll_next_batch(source: StatefulSource, timeout=timedelta(seconds=5)):
    """Repeatedly poll an input source until it returns a batch.

    You'll want to use this in unit tests of sources when there's some
    non-determinism in how items are read.

    Args:

        source: To call `StatefulSource.next` on.

    Returns:

        The next batch found.

    Raises:

        TimeoutError: If no batch was returned within the timeout.

    """
    batch = []
    start = datetime.now(timezone.utc)
    while len(batch) <= 0:
        if datetime.now(timezone.utc) - start > timeout:
            raise TimeoutError()
        batch = source.next_batch()
    return batch
