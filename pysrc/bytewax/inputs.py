"""Dataflow input sources.

Bytewax provides pre-packaged output configuration options for common
sources you might want to read dataflow input from.

Use
---

Create an `InputConfig` subclass for the source you'd like to read
from. Then pass that config object to the `bytewax.dataflow.Dataflow`
constructor.

"""

from typing import Any, Iterable

from .bytewax import InputConfig, KafkaInputConfig, ManualInputConfig  # noqa: F401


def TestingInputConfig(it):
    """Produce input from this Python iterable. You only want to use this
    for unit testing.

    The iterable must be identical on all workers; this will
    automatically distribute the items across workers.

    The value `None` in the iterable will be ignored. See
    `ManualInputConfig` for more info.

    Args:

        it: Iterable for input.

    Returns:

        Config object. Pass this to the `bytewax.dataflow.Dataflow`
        constructor.

    """
    return ManualInputConfig(lambda wi, wn: distribute(it, wi, wn))


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
