"""Helpers to let you quickly define epoch / batching semantics.

Use these to wrap an existing iterator which yields items.
"""
import time
from typing import Any, Iterable, Tuple


def single_batch(wrap_iter: Iterable, epoch: int = 0) -> Iterable[Tuple[int, Any]]:
    """All input items are part of the same epoch.

    Use this for non-streaming-style batch processing.

    >>> list(single_batch(["a", "b", "c"]))
    [(0, 'a'), (0, 'b'), (0, 'c')]

    Args:
        wrap_iter: Existing input iterable of just items.
        epoch: The time value of this epoch.

    Yields: Tuples of (epoch, item).
    """
    for item in wrap_iter:
        yield (epoch, item)


def tumbling_epoch(
    epoch_length_sec: float, wrap_iter: Iterable
) -> Iterable[Tuple[int, Any]]:
    """All inputs within a tumbling window are part of the same epoch.

    >>> import pytest; pytest.skip("Figure out sleep in test.")
    >>> items = [
    ...     "a", # sleep(4)
    ...     "b", # sleep(1)
    ...     "c"
    ... ]
    >>> list(tumbling_epoch(2.0, items))
    [(0, 'a'), (2, 'b'), (2, 'c')]

    Args:
        epoch_length_sec: Length of each epoch window in seconds.
        wrap_iter: Existing input iterable of just items.

    Yields: Tuples of (epoch, item).
    """
    epoch = 0
    last_epoch_start_sec = time.time()
    for item in wrap_iter:
        yield (epoch, item)
        now_sec = time.time()
        frac_intervals = (now_sec - last_epoch_start_sec) / epoch_length_sec
        if frac_intervals >= 1.0:
            epoch += int(frac_intervals)
            last_epoch_start_sec = now_sec


def fully_ordered(wrap_iter: Iterable) -> Iterable[Tuple[int, any]]:
    """Each input item increments the epoch.

    Be carful using this in high-volume streams with many workers, as
    the worker overhead goes up with finely granulated epochs.

    >>> list(fully_ordered(["a", "b", "c"]))
    [(0, 'a'), (1, 'b'), (2, 'c')]

    Args:
        wrap_iter: Existing input iterable of just items.

    Yields: Tuples of (epoch, item).
    """
    epoch = 0
    for item in wrap_iter:
        yield (epoch, item)
        epoch += 1
