"""Helper tools for testing dataflows.
"""
import multiprocessing.dummy
from contextlib import contextmanager
from threading import Lock

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
