"""Helpers to let you quickly define epoch / batching semantics.

Use these to wrap an existing iterator which yields items.

"""

import datetime
import heapq
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Tuple
from .bytewax import AdvanceTo, Emit, KafkaInputConfig, ManualInputConfig, InputConfig


def distribute(elements: Iterable[Any], index: int, count: int) -> Iterable[Any]:
    """Distribute elements equally between a number of buckets and return
    the items for the given bucket index.

    No two buckets will get the same element.

    >>> list(distribute(["blue", "green", "red"], 0, 2))
    ['blue', 'red']
    >>> list(distribute(["blue", "green", "red"], 1, 2))
    ['green']

    Note that if you have more buckets than elements, some buckets
    will get nothing.

    >>> list(distribute(["blue", "green", "red"], 3, 5))
    []

    This is very useful when writing input builders and you want each
    of your workers to handle reading a disjoint partition of your
    input.

    >>> from bytewax import Dataflow, spawn_cluster
    >>> from bytewax.inputs import ManualInputConfig
    >>> def read_topics(topics):
    ...     for topic in topics:
    ...         for i in enumerate(3):
    ...             yield f"topic:{topic} item:{i}"
    >>> def input_builder(i, n):
    ...    all_topics = ["red", "green", "blue"]
    ...    this_workers_topics = distribute(listening_topics, i, n)
    ...    for item in read_topics(this_workers_topics):
    ...        yield Emit(f"worker_index:{worker_index}" + item)
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> spawn_cluster(flow, ManualInputConfig(input_builder), lambda i, n: print)
    (0, 'worker_index:1 topic:red item:0')
    (0, 'worker_index:1 topic:red item:1')
    (0, 'worker_index:1 topic:red item:2')
    (0, 'worker_index:1 topic:blue item:0')
    (0, 'worker_index:1 topic:blue item:1')
    (0, 'worker_index:1 topic:blue item:2')
    (0, 'worker_index:2 topic:green item:0')
    (0, 'worker_index:2 topic:green item:1')
    (0, 'worker_index:2 topic:green item:2')

    Args:

        elements: To distribute.

        index: Index of this bucket / worker starting at 0.

        count: Total number of buckets / workers.

    Returns:

        An iterator of the elements only in this bucket.

    """
    assert index < count, f"Highest index should only be {count - 1}; got {index}"
    for i, x in enumerate(elements):
        if i % count == index:
            yield x


def yield_epochs(fn: Callable):
    """A decorator function to unwrap an iterator of [epoch, item]
    into successive `AdvanceTo` and `Emit` classes with the
    contents of the iterator.

    Use this when you have an input_builder function that returns a
    generator of (epoch, item) to be used with
    `bytewax.cluster_main()` or `bytewax.spawn_cluster()`:

    >>> from bytewax import Dataflow, cluster_main
    >>> from bytewax.inputs import yield_epochs, fully_ordered, ManualInputConfig
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> @yield_epochs
    ... def input_builder(i, n, re):
    ...   return fully_ordered(["a", "b", "c"])
    >>> cluster_main(flow, ManualInputConfig(input_builder), lambda i, n: print, [], 0, 1)
    (0, 'a')
    (1, 'b')
    (2, 'c')

    """

    def inner_fn(worker_index, worker_count, resume_epoch):
        gen = fn(worker_index, worker_count, resume_epoch)
        for (epoch, item) in gen:
            yield AdvanceTo(epoch)
            yield Emit(item)

    return inner_fn


def single_batch(wrap_iter: Iterable) -> Iterable[Tuple[int, Any]]:
    """All input items are part of the same epoch.

    Use this for non-streaming-style batch processing.

    >>> from bytewax import Dataflow, run
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> out = run(flow, single_batch(["a", "b", "c"]))
    >>> sorted(out)
    [(0, 'a'), (0, 'b'), (0, 'c')]

    Args:

        wrap_iter: Existing input iterable of just items.

    Yields:

        Tuples of `(epoch, item)`.

    """
    for item in wrap_iter:
        yield (0, item)


def tumbling_epoch(
    wrap_iter: Iterable,
    epoch_length: Any,
    time_getter: Callable[[Any], Any] = lambda _: datetime.datetime.now(),
    epoch_start_time: Any = None,
    epoch_start: int = 0,
) -> Iterable[Tuple[int, Any]]:
    """All inputs within a tumbling window are part of the same epoch.

    The time of the first item will be used as start of the 0
    epoch. Out-of-order items will cause issues as Bytewax requires
    inputs to dataflows to be in epoch order. See
    `bytewax.inputs.fully_ordered()`.

    >>> from bytewax import Dataflow, run
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
    >>> flow = Dataflow()
    >>> flow.map(lambda item: item["value"])
    >>> flow.capture()
    >>> out = run(flow, tumbling_epoch(
    ...     items,
    ...     datetime.timedelta(seconds=2),
    ...     lambda item: item["timestamp"],
    ... ))
    >>> sorted(out)
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
    [(0, 'a'), (2, 'b'), (2, 'c')]

    Args:

        wrap_iter: Existing input iterable of just items.

        epoch_length: Length of each epoch window.

        time_getter: Function that returns a timestamp given an
            item. Defaults to current wall time.

        epoch_start_time: The timestamp that should correspond to
            the start of the 0th epoch. Otherwise defaults to the time
            found on the first item.

        epoch_start: The integer value to start counting epochs from.
            This can be used for continuity during processing.

    Yields:

        Tuples of `(epoch, item)`.

    """
    for item in wrap_iter:
        time = time_getter(item)

        if epoch_start_time is None:
            epoch_start_time = time
            epoch = epoch_start
        else:
            epoch = int((time - epoch_start_time) / epoch_length) + epoch_start

        yield (epoch, item)


def fully_ordered(wrap_iter: Iterable) -> Iterable[Tuple[int, Any]]:
    """Each input item increments the epoch.

    Be careful using this in high-volume streams with many workers, as
    the worker overhead goes up with finely granulated epochs.

    >>> from bytewax import Dataflow, run
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> out = run(flow, fully_ordered(["a", "b", "c"]))
    >>> sorted(out)
    [(0, 'a'), (1, 'b'), (2, 'c')]

    Args:

        wrap_iter: Existing input iterable of just items.

    Yields:

        Tuples of `(epoch, item)`.

    """
    epoch = 0
    for item in wrap_iter:
        yield (epoch, item)
        epoch += 1


@dataclass
class _HeapItem:
    """Wrapper class which holds pairs of time and item for implementing
    `sorted_window()`.

    We need some class that has an ordering only based on the time.

    """

    time: Any
    item: Any

    def __lt__(self, other):
        """Compare just by timestamp. Ignore the item."""
        return self.time < other.time


def sorted_window(
    wrap_iter: Iterable,
    window_length: Any,
    time_getter: Callable[[Any], Any],
    on_drop: Callable[[Any], None] = None,
) -> Iterable[Tuple[int, Any]]:
    """Sort a iterator to be increasing by some timestamp.

    To support a possibly infinite iterator, store a limited sorted
    buffer of items and only emit things downstream once a certain
    window of time has passed, as indicated by the timestamp on new
    items.

    New input items which are older than those already emitted will be
    dropped to maintain sorted output.

    The window length needs to be tuned for how "out of order" your
    input data is and how much data you're willing to drop: Already
    perfectly ordered input data can have a window of "0" and nothing
    will be dropped. Completely reversed input data needs a window
    that is the difference between the oldest and youngest timestamp
    to ensure nothing will be dropped.

    >>> from bytewax import Dataflow, run
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
    >>> sorted_items = list(
    ...     sorted_window(
    ...         items,
    ...         datetime.timedelta(seconds=2),
    ...         lambda item: item["timestamp"],
    ...     )
    ... )
    >>> sorted_items
    [{'timestamp': datetime.datetime(2022, 2, 22, 1, 2, 3), 'value': 'b'},
    {'timestamp': datetime.datetime(2022, 2, 22, 1, 2, 4), 'value': 'c'}]

    You could imagine using it with `tumbling_epoch()` to ensure you
    get in-order, bucketed data into your dataflow.

    >>> flow = Dataflow()
    >>> flow.map(lambda item: item["value"])
    >>> flow.capture()
    >>> out = run(flow, tumbling_epoch(
    ...     sorted_items,
    ...     datetime.timedelta(seconds=0.5),
    ...     lambda item: item["timestamp"],
    ... ))
    >>> sorted(out)
    [(0, 'b'), (2, 'c')]

    Args:

        wrap_iter: Existing input iterable.

        window_length: Buffering duration. Values will be emitted once
            this amount of time has passed.

        time_getter: Function to call to produce a timestamp for each
            value.

        on_drop: Function to call with each dropped item. E.g. log or
            increment metrics on drop events to refine your window
            length.

    Yields:

        Values in increasing timestamp order.

    """
    sorted_buffer = []
    newest_time = None
    drop_older_than = None

    def is_too_late(time):
        return drop_older_than is not None and time <= drop_older_than

    def is_newest_item(time):
        return newest_time is None or time > newest_time

    def emit_all(emit_older_than):
        while len(sorted_buffer) > 0 and sorted_buffer[0].time <= emit_older_than:
            sort_item = heapq.heappop(sorted_buffer)
            yield sort_item.item

    for item in wrap_iter:
        time = time_getter(item)

        if is_too_late(time):
            if on_drop:
                on_drop(item)
        else:
            heapq.heappush(sorted_buffer, _HeapItem(time, item))

            if is_newest_item(time):
                newest_time = time
                drop_older_than = time - window_length

                yield from emit_all(drop_older_than)

    yield from emit_all(newest_time)
