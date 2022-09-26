"""Dataflow input sources.

Bytewax provides pre-packaged input configuration options for common
sources you might want to read dataflow input from.

Use
---

Create an `InputConfig` subclass for the source you'd like to read
from. Then pass that config object as the `input_config` argument of
the `bytewax.dataflow.Dataflow.input` operator.

"""

from typing import Any, Callable, Iterable

from .bytewax import InputConfig, KafkaInputConfig, ManualInputConfig  # noqa: F401


class TestingBuilderInputConfig(ManualInputConfig):
    """Produce input from a builder of a Python iterable. You only want to
    use this for unit testing.

    The iterable must be identical on all workers; this will
    automatically distribute the items across workers and handle
    recovery.

    Args:

        builder: Called upon dataflow startup which returns an
            iterable for input.

    Returns:

        Config object. Pass this as the `input_config` argument of the
        `bytewax.dataflow.Dataflow.input` operator.

    """

    # This is needed to avoid pytest trying to load this class as
    # a test since its name starts with "Test"
    __test__ = False

    def __new__(cls, builder: Callable[[], Iterable[Any]]):
        def gen(worker_index, worker_count, resume_state):
            it = builder()
            resume_i = resume_state or 0
            for i, x in enumerate(distribute(it, worker_index, worker_count)):
                # FFWD to the resume item.
                if i < resume_i:
                    continue
                # Store the index in this worker's partition as the resume
                # state.
                yield (i + 1, x)

        return super().__new__(cls, gen)


class TestingInputConfig(TestingBuilderInputConfig):
    """Produce input from a Python iterable. You only want to use this for
    unit testing.

    The iterable must be identical on all workers; this will
    automatically distribute the items across workers and handle
    recovery.

    Be careful using a generator as the iterable; if you fail and
    attempt to resume the dataflow without rebuilding it, the
    half-consumed generator will be re-used on recovery and unexpected
    input will be used. See `TestingBuilderInputConfig`.

    Args:

        it: Iterable for input.

    Returns:

        Config object. Pass this as the `input_config` argument of the
        `bytewax.dataflow.Dataflow.input` operator.

    """

    def __new__(cls, it: Iterable[Any]):
        return super().__new__(cls, lambda: it)


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

    For example this code:

    ```python
    from bytewax.dataflow import Dataflow
    from bytewax.execution import spawn_cluster
    from bytewax.inputs import ManualInputConfig, distribute
    from bytewax.outputs import StdOutputConfig

    def read_topics(topics):
       for topic in topics:
            for i in range(3):
                yield f"topic:{topic} item:{i}"

    def input_builder(worker_index, workers_count, resume_state):
        state = None
        all_topics = ["red", "green", "blue"]
        this_workers_topics = distribute(all_topics, worker_index, workers_count)
        for item in read_topics(this_workers_topics):
            yield (state, f"worker_index:{worker_index} {item}")

    flow = Dataflow()
    flow.input("input", ManualInputConfig(input_builder))
    flow.capture(StdOutputConfig())
    spawn_cluster(flow)
    ```

    Outputs (not in this order):

    ```
    worker_index:0 topic:red item:0
    worker_index:0 topic:red item:1
    worker_index:0 topic:red item:2
    worker_index:0 topic:blue item:0
    worker_index:0 topic:blue item:1
    worker_index:0 topic:blue item:2
    worker_index:1 topic:green item:0
    worker_index:1 topic:green item:1
    worker_index:1 topic:green item:2
    ```


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
