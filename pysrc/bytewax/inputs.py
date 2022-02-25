"""Helpers to let you quickly define epoch / batching semantics.

Use these to wrap an existing iterator which yields items.
"""
import datetime
import heapq
from operator import itemgetter
from typing import Any, Iterable, Tuple, Callable


def single_batch(wrap_iter: Iterable) -> Iterable[Tuple[int, Any]]:
    """All input items are part of the same epoch.

    Use this for non-streaming-style batch processing.

    >>> list(single_batch(["a", "b", "c"]))
    [(0, 'a'), (0, 'b'), (0, 'c')]

    Args:

        wrap_iter: Existing input iterable of just items.

    Yields: Tuples of (epoch, item).
    """
    for item in wrap_iter:
        yield (0, item)
        

def order(
        wrap_iter: Iterable,
        window_length: Any,
        time_getter: Callable[[Any], Any] = itemgetter(0),
        on_drop: Callable[[Any], None] = None,
) -> Iterable[Tuple[int, Any]]:
    """Order out-of-order values within a sliding window.

    Values are emitted in order after seeing a new time that would
    push them out of the sliding window. Values that arrive after they
    would have been emitted are too late and are dropped.

    Use if you have a possibly out-of-order data source as Bytewax
    requires inputs to dataflows to be in epoch order.

    >>> items = [
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4),
    ...         "value": "c",
    ...     },
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3),
    ...         "value": "b",
    ...     },
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 0),
    ...         "value": "a",
    ...     },
    ... ]
    >>> [
    ...     item["value"]
    ...     for item
    ...     in order(
    ...         items,
    ...         datetime.timedelta(seconds=2),
    ...         itemgetter("timestamp"),
    ...     )
    ... ]
    ['b', 'c']

    Args:
    
        wrap_iter: Existing input iterable.
   
        window_length: Maximum out-of-order-ness before dropping
            values.

        time_getter: Function to call to produce a timestamp for each
            value. Defaults to using the first item in each value;
            e.g. `(time, x)`.

        on_drop: Function to call with each dropped item. Defaults to
            nothing.

    Yields: Values in order.
    """
    buffer_heap = []
    buffer_close_time = None
    for item in wrap_iter:
        time = time_getter(item)
        if buffer_close_time is None or time >= buffer_close_time:
            heapq.heappush(buffer_heap, (time, item))

            buffer_close_time_item = time - window_length
            if buffer_close_time is None or buffer_close_time_item > buffer_close_time:
                buffer_close_time = buffer_close_time_item
        
                while len(buffer_heap) > 0 and buffer_heap[0][0] < buffer_close_time:
                    time, item = heapq.heappop(buffer_heap)
                    yield item
            # Else: This item didn't push forward the window, so no
            # items could be emitted.
        elif on_drop:
            on_drop(item)

    # Emit everything remaining.
    while len(buffer_heap) > 0:
        time, item = heapq.heappop(buffer_heap)
        yield item
        

def tumbling_epoch(
        wrap_iter: Iterable,
        epoch_length: Any,
        time_getter: Callable[[Any], Any] = lambda _: datetime.datetime.now(),
        epoch_0_start_time: Any = None,
) -> Iterable[Tuple[int, Any]]:
    """All inputs within a tumbling window are part of the same epoch.

    The time of the first item will be used as start of the 0
    epoch. Out-of-order items will cause issues as Bytewax requires
    inputs to dataflows to be in epoch order. See `order()`.

    >>> items = [
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3),
    ...         "value": "a",
    ...     },
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4),
    ...         "value": "b",
    ...     },
    ...     {
    ...         "timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8),
    ...         "value": "c",
    ...     },
    ... ]
    >>> [
    ...     (epoch, item["value"])
    ...     for epoch, item
    ...     in tumbling_epoch(
    ...         items,
    ...         datetime.timedelta(seconds=2),
    ...         itemgetter("timestamp"),
    ...     )
    ... ]
    [(0, 'a'), (0, 'b'), (2, 'c')]

    By default, uses "ingestion time" and you don't need to specify a
    way to access the timestamp in each item.

    >>> import pytest; pytest.skip("Figure out sleep in test.")
    >>> items = [
    ...     "a", # sleep(4)
    ...     "b", # sleep(1)
    ...     "c",
    ... ]
    >>> list(tumbling_epoch(items, datetime.timedelta(seconds=2)))
    [(0, 'a'), (0, 'b'), (2, 'c')]

    Args:

        wrap_iter: Existing input iterable of just items.

        epoch_length: Length of each epoch window.

        time_getter: Function that returns a timestamp given an
            item. Defaults to current wall time.

        epoch_0_start_time: The timestamp that should correspond to
            the start of the 0th epoch. Otherwise defaults to the time
            found on the first item.

    Yields: Tuples of (epoch, item).
    """
    for item in wrap_iter:
        time = time_getter(item)
        
        if epoch_0_start_time is None:
            epoch_0_start_time = time
            epoch = 0
        else:
            epoch = int((time - epoch_0_start_time) / epoch_length)

        yield (epoch, item)

                      
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
