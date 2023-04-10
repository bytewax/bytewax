"""Low-level output interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom sink.

"""

from abc import abstractmethod
from typing import Any, Optional, Set


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class Output:
    """Base class for all output types. Do not subclass this.

    If you want to implement a custom connector, instead subclass one
    of the specific output sub-types below in this module.

    """

    def __json__(self):
        """This is used by the Bytewax platform internally and should
        not be overridden.

        """
        return {
            "type": type(self).__name__,
        }


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class StatefulSink:
    """Output sink that maintains state of its position."""

    @abstractmethod
    def write(self, value) -> None:
        """Write a single output value.

        Called once with only `value` for each `(key, value)` at this
        point in the dataflow.

        See `PartitionedOutput.assign_part` for how the key is mapped
        to partition.

        Args:

            value: Value in the dataflow.

        """
        ...

    @abstractmethod
    def snapshot(self) -> Any:
        """Snapshot the position of the next write of this sink.

        This will be returned to you via the `resume_state` parameter
        of your output builder.

        Be careful of "off by one" errors in resume state. This should
        return a state that, when built into a sink, resumes writing
        _after the last written item_, not overwriting the same item.

        This is guaranteed to never be called after `close()`.

        Returns:

            Resume state.

        """
        ...

    def close(self) -> None:
        """Do any cleanup on this sink when the dataflow completes on
        a finite input.

        This is not guaranteed to be called. It will not be called
        during a crash.

        """
        pass


class PartitionedOutput(Output):
    """An output with a fixed number of independent partitions.

    Will maintain the state of each sink and re-build using it during
    resume. If the sink supports seeking and overwriting, this output
    can support exactly-once processing.

    """

    @abstractmethod
    def list_parts(self) -> Set[str]:
        """List all partitions by a string key.

        This must consistently return the same keys when called by all
        workers in all executions.

        Keys must be unique within this dataflow step.

        Returns:

            Partition keys.

        """
        ...

    @abstractmethod
    def assign_part(self, item_key: str) -> str:
        """Define how incoming `(key, value)` pairs should be routed
        to partitions.

        This must be globally consistent and return the same partition
        assignment on every call.

        .. caution:: Do not use Python's built in `hash` function
            here! It is [_not consistent between processes by
            default_](https://docs.python.org/3/using/cmdline.html#cmdoption-R)
            and using it will cause incorrect partitioning in cluster
            executions.

            You can start by using `zlib.adler32` as a quick drop-in
            replacement.

        Args:

            item_key: Key that is about to be written.

        Returns:

            Partition key the value for this key should be written
            to. Must be one of the partition keys returned by
            `list_parts`.

        """
        ...

    @abstractmethod
    def build_part(
        self,
        for_part: str,
        resume_state: Optional[Any],
    ) -> Optional[StatefulSink]:
        """Build an output partition, resuming writing at the position
        encoded in the resume state.

        Will be called once on one worker in an execution for each
        partition key in order to distribute partitions across all
        workers.

        Return `None` if for some reason this partition is no longer
        valid and can be skipped coherently. Raise an exception if
        not.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.

        Args:

            for_part: Which partition to build.

            resume_state: State data containing where in the output
                stream this partition should be begin writing during
                this execution.

        Returns:

            The built partition, or `None`.

        """
        ...


# TODO: Add ABC superclass. It messes up pickling. We should get rid
# of pickling...
class StatelessSink:
    """Output sink that is stateless."""

    @abstractmethod
    def write(self, item) -> None:
        """

        Called once for each item at this point in the dataflow.

        Args:

            item: Item in the dataflow.
        """
        ...

    def close(self) -> None:
        """Do any cleanup on this sink when the dataflow completes on
        a finite input.

        This is not guaranteed to be called. It will not be called
        during a crash.

        """
        pass


class DynamicOutput(Output):
    """An output that supports writing from any number of workers
    concurrently.

    Does not support storing any resume state. Thus these kind of
    outputs only naively can support at-least-once processing.

    """

    @abstractmethod
    def build(self, worker_index, worker_count) -> StatelessSink:
        """Build an output sink for a worker.

        Will be called once on each worker.

        Args:

            worker_index: Index of this worker.

            worker_count: Total number of workers.

        Returns:

            Output sink.

        """
        ...
