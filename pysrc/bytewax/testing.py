"""Helper tools for testing dataflows."""
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any, Iterable, Iterator

from bytewax.inputs import (
    DynamicInput,
    PartitionedInput,
    StatefulSource,
    StatelessSource,
    batch,
)
from bytewax.outputs import DynamicOutput, StatelessSink

from .bytewax import cluster_main, run_main

__all__ = [
    "run_main",
    "cluster_main",
    "ffwd_iter",
    "poll_next_batch",
    "TestingInput",
    "TestingOutput",
]


def ffwd_iter(it: Iterator[Any], n: int) -> None:
    """Skip an iterator forward some number of items.

    Args:
        it:
            A stateful iterator to advance.
        n:
            Number of items to skip from the current position.

    """
    # Taken from `consume`
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    # Apparently faster than a for loop.
    next(islice(it, n, n), None)


class _IterSource(StatefulSource):
    def __init__(self, ib, batch_size, resume_state):
        self._start_idx = 0 if resume_state is None else resume_state
        it = iter(ib)
        # Resume to one after the last completed read index.
        ffwd_iter(it, self._start_idx)
        self._batcher = batch(it, batch_size)

    def next_batch(self):
        batch = next(self._batcher)
        self._start_idx += len(batch)
        return batch

    def snapshot(self):
        return self._start_idx


class TestingInput(PartitionedInput):
    """Produce input from a Python iterable.

    You only want to use this for unit testing.

    The iterable must be identical on all workers.

    There is no parallelism; only one worker will actually consume the
    iterable.

    Be careful using a generator as the iterable; if you fail and
    attempt to resume the dataflow without rebuilding it, the
    half-consumed generator will be re-used on recovery and early
    input will be lost so resume will see the correct data.

    """

    __test__ = False

    def __init__(self, ib: Iterable[Any], batch_size: int = 1):
        """Init.

        Args:
            ib:
                Iterable for input.
            batch_size:
                Number of items from the iterable to emit in each
                batch. Defaults to 1.

        """
        self._ib = ib
        self._batch_size = batch_size

    def list_parts(self):
        """The iterable is read on a single worker."""
        return ["iterable"]

    def build_part(self, for_key, resume_state):
        """See ABC docstring."""
        assert for_key == "iterable"
        return _IterSource(self._ib, self._batch_size, resume_state)


class _ListSink(StatelessSink):
    def __init__(self, ls):
        self._ls = ls

    def write_batch(self, items):
        self._ls += items


class TestingOutput(DynamicOutput):
    """Append each output item to a list.

    You only want to use this for unit testing.

    Can support at-least-once processing. The list is not cleared
    between executions.

    """

    __test__ = False

    def __init__(self, ls):
        """Init.

        Args:
            ls: List to append to.
        """
        self._ls = ls

    def build(self, worker_index, worker_count):
        """See ABC docstring."""
        return _ListSink(self._ls)


def poll_next_batch(source: StatefulSource, timeout=timedelta(seconds=5)):
    """Repeatedly poll an input source until it returns a batch.

    You'll want to use this in unit tests of sources when there's some
    non-determinism in how items are read.

    This is a busy-loop.

    Args:
        source: To call `StatefulSource.next` on.

        timeout: How long to continuously poll for.

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


class _PeriodicSource(StatelessSource):
    def __init__(self, should_emit, frequency, limit, cb):
        self._should_emit = should_emit
        self._counter = 0
        self._next_awake = datetime.now(timezone.utc)
        self._frequency = frequency
        self._limit = limit
        self._cb = cb

    def next_batch(self):
        if not self._should_emit:
            raise StopIteration()

        self._next_awake += self._frequency
        self._counter += 1

        if self._limit is not None and self._counter >= self._limit:
            raise StopIteration()

        return [self._cb(self._counter)]

    def next_awake(self):
        return self._next_awake


class TestingPeriodicInput(DynamicInput):
    """An input that emits an increasing counter at the requested frequency.

    The counter can be optionally passed to a user provided callback
    so that items can be modified as needed before being emitted.

    Elements are only emitted on the first worker, all other workers do nothing.

    Args:
        frequency: The period at which items will be emitted.

        limit: [optional] how many items to emit before stopping.

        cb: [optional] a callback that will be called for each item with the
            item itself as first and only argument
    """

    __test__ = False

    def __init__(self, frequency, *, limit=None, cb=None):
        self._frequency = frequency
        self._limit = limit

        def default_cb(x):
            return x

        self._cb = cb or default_cb

    def build(self, worker_index, worker_count):
        return _PeriodicSource(
            worker_index == 0, self._frequency, self._limit, self._cb
        )
