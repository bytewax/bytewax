"""Low-level input interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom source.

"""

from abc import ABC, abstractmethod
from typing import Any, List, Optional

__all__ = [
    "DynamicInput",
    "Input",
    "PartitionedInput",
    "StatefulSource",
    "StatelessSource",
]


class Input(ABC):
    """Base class for all input types. Do not subclass this.

    If you want to implement a custom connector, instead subclass one
    of the specific input sub-types below in this module.

    """

    def __json__(self):
        """This is used by the Bytewax platform internally and should
        not be overridden.

        """
        return {
            "type": type(self).__name__,
        }


class StatefulSource(ABC):
    """Input source that maintains state of its position."""

    @abstractmethod
    def next_batch(self) -> List[Any]:
        """Attempt to get the next batch of input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but returning an empty list if there are no
        items to emit yet.

        Returns:

            An list of items immediately ready. May be empty if no new
            items.

        Raises:

            StopIteration: When the source is complete.

        """
        ...

    @abstractmethod
    def snapshot(self) -> Any:
        """Snapshot the position of the next read of this source.

        This will be returned to you via the `resume_state` parameter
        of your input builder.

        Be careful of "off by one" errors in resume state. This should
        return a state that, when built into a source, resumes reading
        _after the last read item item_, not the same item that
        `next()` last returned.

        This is guaranteed to never be called after `close()`.

        Returns:

            Resume state.

        """
        ...

    def close(self) -> None:
        """Do any cleanup on this source when the dataflow completes
        on a finite input.

        This is not guaranteed to be called. It will not be called
        during a crash.

        """
        pass


class PartitionedInput(Input):
    """An input with a fixed number of independent partitions.

    Will maintain the state of each source and re-build using it
    during resume. If the source supports seeking, this input can
    support exactly-once processing.

    Each partition must contain unique data. If you re-read the same data
    in multiple partitions, the dataflow will process these duplicate
    items.

    """

    @abstractmethod
    def list_parts(self) -> List[str]:
        """List all local partitions this worker has access to.

        You do not need to list all partitions globally.

        Returns:

            Local partition keys.

        """
        ...

    @abstractmethod
    def build_part(
        self,
        for_part: str,
        resume_state: Optional[Any],
    ) -> StatefulSource:
        """Build an input partition, resuming from the position
        encoded in the resume state.

        Will be called once per execution for each partition key on a
        worker that reported that partition was local in `list_parts`.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.

        Args:

            for_part: Which partition to build. Will always be one of
                the keys returned by `list_parts` on this worker.

            resume_state: State data containing where in the input
                stream this partition should be begin reading during
                this execution.

        Returns:

            The built partition.

        """
        ...


class StatelessSource(ABC):
    """Input source that is stateless."""

    @abstractmethod
    def next_batch(self) -> List[Any]:
        """Attempt to get the next batch of input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but yielding an empty list if there are no new
        items yet.

        Returns:

            An list of items immediately ready. May be empty if no new
            items.

        Raises:

            StopIteration: When the source is complete.

        """
        ...

    def close(self) -> None:
        """Do any cleanup on this source when the dataflow completes
        on a finite input.

        This is not guaranteed to be called. It will not be called
        during a crash.

        """
        pass


class DynamicInput(Input):
    """An input that supports reading distinct items from any number
    of workers concurrently.

    Does not support storing any resume state. Thus these kind of
    inputs only naively can support at-most-once processing.

    The source must somehow support supplying disjoint data for each
    worker. If you re-read the same items on multiple workers, the
    dataflow will process these as duplicate items.

    """

    @abstractmethod
    def build(self, worker_index, worker_count) -> StatelessSource:
        """Build an input source for a worker

        Will be called once on each worker.

        Args:

            worker_index: Index of this worker.

            worker_count: Total number of workers.

        Returns:

            Input source.

        """
        ...
