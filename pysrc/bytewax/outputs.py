"""Low-level output interfaces.

If you want pre-built connectors for various external systems, see
{py:obj}`bytewax.connectors`. That is also a rich source of examples.

"""

from abc import ABC, abstractmethod
from typing import Generic, List, Optional, Tuple, TypeVar
from zlib import adler32

X = TypeVar("X")
"""Type consumed by a {py:obj}`Sink`."""

S = TypeVar("S")
"""Type of state snapshots."""


class Sink(ABC, Generic[X]):  # noqa: B024
    """A destination to write output items.

    Base class for all output sinks. Do not subclass this.

    If you want to implement a custom connector, instead subclass one
    of the specific sink sub-types below in this module.

    """

    pass


class StatefulSinkPartition(ABC, Generic[X, S]):
    """Output partition that maintains state of its position."""

    @abstractmethod
    def write_batch(self, values: List[X]) -> None:
        """Write a batch of output values.

        Called with a list of `value`s for each `(key, value)` at this
        point in the dataflow.

        See {py:obj}`FixedPartitionedSink.part_fn` for how the key is
        mapped to partition.

        :arg values: Values in the dataflow. Non-deterministically
            batched.

        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Snapshot the position of the next write of this partition.

        This will be returned to you via the `resume_state` parameter
        of {py:obj}`FixedPartitionedSink.build_part`.

        Be careful of "off by one" errors in resume state. This should
        return a state that, when built into a partition, resumes writing
        _after the last written item_, not overwriting the same item.

        This is guaranteed to never be called after {py:obj}`close`.

        :returns: Resume state.

        """
        ...

    def close(self) -> None:
        """Cleanup this partition when the dataflow completes.

        This is not guaranteed to be called. It will only be called
        when the dataflow finishes on finite input. It will not be
        called during an abrupt or abort shutdown.

        """
        return


class FixedPartitionedSink(Sink[Tuple[str, X]], Generic[X, S]):
    """An output sink with a fixed number of independent partitions.

    Will maintain the state of each partition and re-build using it during
    resume. If the sink supports seeking and overwriting, this output
    can support exactly-once processing.

    """

    @abstractmethod
    def list_parts(self) -> List[str]:
        """List all local partitions this worker has access to.

        You do not need to list all partitions globally.

        :returns: Local partition keys.

        """
        ...

    def part_fn(self, item_key: str) -> int:
        """Route incoming `(key, value)` pairs to partitions.

        Defaults to {py:obj}`zlib.adler32` as a simple consistent
        function.

        This must be globally consistent across workers and executions
        and return the same hash on every call.

        A specific partition is chosen by wrapped indexing this value
        into the ordered global set of partitions. (Not just
        partitions local to this worker.)

        :::{caution}

        Do not use Python's built in {py:obj}`hash` function here! It
        is [_not consistent between processes by
        default_](inv:python:std:cmdoption#-R) and using it will cause
        incorrect partitioning in cluster executions.

        :::

        :arg item_key: Key for the value that is about to be written.

        :returns: Integer hash value that is used to assign partition.

        """
        return adler32(item_key.encode())

    @abstractmethod
    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[S],
    ) -> StatefulSinkPartition[X, S]:
        """Build anew or resume an output partition.

        Will be called once per execution for each partition key on a
        worker that reported that partition was local in
        {py:obj}`list_parts`.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.

        :arg step_id: The step_id of the output operator.

        :arg for_part: Which partition to build. Will always be one of
            the keys returned by {py:obj}`list_parts` on this worker.

        :arg resume_state: State data containing where in the output
            stream this partition should be begin writing during this
            execution.

        :returns: The built partition.

        """
        ...


class StatelessSinkPartition(ABC, Generic[X]):
    """Output partition that is stateless."""

    @abstractmethod
    def write_batch(self, items: List[X]) -> None:
        """Write a batch of output items.

        Called multiple times whenever new items are seen at this
        point in the dataflow.

        :arg items: Items in the dataflow. Non-deterministically
            batched.

        """
        ...

    def close(self) -> None:
        """Cleanup this partition when the dataflow completes.

        This is not guaranteed to be called. It will only be called
        when the dataflow finishes on finite input. It will not be
        called during an abrupt or abort shutdown.

        """
        return


class DynamicSink(Sink[X]):
    """An output sink where all workers write items concurrently.

    Does not support storing any resume state. Thus these kind of
    outputs only naively can support at-least-once processing.

    """

    @abstractmethod
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSinkPartition[X]:
        """Build an output partition for a worker.

        Will be called once on each worker.

        :arg step_id: The step_id of the output operator.

        :arg worker_index: Index of this worker.

        :arg worker_count: Total number of workers.

        :returns: The built partition.

        """
        ...
