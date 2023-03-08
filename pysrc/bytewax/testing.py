"""Helper tools for testing dataflows.
"""
import multiprocessing.dummy
from contextlib import contextmanager
from threading import Lock
from typing import Any, Iterable

from bytewax.inputs import PartitionedInput
from bytewax.outputs import DynamicOutput


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
        self.it = it

    def list_parts(self):
        return {"iter"}

    def build_part(self, for_key, resume_state):
        assert for_key == "iter"
        resume_i = resume_state or -1

        for i, x in enumerate(self.it):
            # Resume to one after the last completed read.
            if i <= resume_i:
                continue
            yield i, x


class TestingOutput(DynamicOutput):
    """Append each output item to a list. You only want to use this
    for unit testing.

    Can support at-least-once processing. The list is not cleared
    between executions.

    Be careful using this in multi-worker executions: all workers will
    write to the same list. You'll probably want to use
    `multiprocessing.Manager.List` to ensure all worker's writes are
    visible in the test process.

    Args:

        ls: List to append to.

    """

    __test__ = False

    def __init__(self, ls):
        self.ls = ls

    def build(self):
        return self.ls.append


_print_lock = Lock()


def test_print(*args, **kwargs):
    """A version of `print()` which takes an in-process lock to prevent
    multiple worker threads from writing simultaneously which results
    in interleaved output.

    You'd use this if you're integration testing a dataflow and want
    more deterministic output. Remember that even with this, the items
    from multi-worker output might be "out-of-order" because each
    worker is racing each other. You probably want to sort your output
    in some way.

    Arguments are passed through to `print()` unmodified.

    """
    with _print_lock:
        print(*args, flush=True, **kwargs)


doctest_ctx = multiprocessing.dummy
"""Use this `multiprocessing` context when running in doctests.

Pass to `bytewax.spawn_cluster()` and `bytewax.run_cluster()`.

Spawning subprocesses is fraught in doctest contexts, so use this to
demonstrate the API works, but not actually run via multiple
processes. We have other normal `pytest` tests which actually test
behavior. Don't worry.

"""


@contextmanager
def _Manager():
    """`multiprocessing.dummy.Manager()` doesn't support being a context
    manager like a real `multiprocessing.Manager()` does... So let's
    monkey patch it.

    """
    yield doctest_ctx


doctest_ctx.Manager = _Manager
