"""Low-level input interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom source.

"""

import asyncio
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any, List, Optional, Set

__all__ = [
    "AsyncBatcher",
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
    def next(self) -> List[Any]:
        """Attempt to get the next input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but returning an empty list if there are no items
        to emit currently.

        Returns:

            A list of items.

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
    def build_part(
        self,
        for_part: str,
        resume_state: Optional[Any],
    ) -> Optional[StatefulSource]:
        """Build an input partition, resuming from the position
        encoded in the resume state.

        Will be called once within each cluster for each partition
        key.

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

            resume_state: State data containing where in the input
                stream this partition should be begin reading during
                this execution.

        Returns:

            The built partition, or `None`.

        """
        ...


class StatelessSource(ABC):
    """Input source that is stateless."""

    @abstractmethod
    def next(self) -> List[Any]:
        """Attempt to get the next input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but returning an empty list if there is no new
        input.

        Returns:

            A list of items.

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


class AsyncBatcher:
    """Allows you to poll an async iterator synchronously up to a
    given time and length limit.

    This is great for allowing async input sources to play well with
    Bytewax's requirement that calling `next` on sources should never
    block.

    Args:

        aiter: The underlying source async iterator of items.

        loop: Custom `asyncio` run loop to use, if any.

    """

    def __init__(self, aiter: AsyncIterator[Any], loop=None):
        self._aiter = aiter

        self._loop = loop if loop is not None else asyncio.new_event_loop()
        self._task = None

    def next_batch(self, timeout: timedelta, max_len: int = sys.maxsize) -> List[Any]:
        """Gather a batch of items up to some limits.

        Args:

            timeout: Duration of time to repeatedly poll the source
                async iterator for items.

            max_len: Maximum number of items to return, even if the
                timeout has not been hit. Defaults to "no limit".

        Returns:

            The gathered batch of items.

            This function will take up to `timeout` time to return, or
            will return a list with length up to `max_len`.

        Raises:

            StopIteration: When there are no more batches.

        """

        async def anext_batch():
            batch = []
            # Only try to gather this many items.
            for _ in range(max_len):
                if self._task is None:
                    self._task = self._loop.create_task(self._aiter.__anext__())

                try:
                    # Prevent the `wait_for` cancellation from
                    # stopping the `__anext__` task; usually all
                    # sub-tasks are cancelled too. It'll be re-used in
                    # the next batch.
                    next_item = await asyncio.shield(self._task)
                except asyncio.CancelledError:
                    # Timeout was hit and thus return the batch
                    # immediately.
                    break
                except StopAsyncIteration:
                    if len(batch) > 0:
                        # Return a half-finished batch if we run out
                        # of source items.
                        break
                    else:
                        # We can't raise `StopIteration` directly here
                        # because it's part of the coro protocol and
                        # would mess with this async function.
                        raise

                batch.append(next_item)
                self._task = None
            return batch

        try:
            # `wait_for` will raise `CancelledError` at the internal
            # await point in `anext_batch` if the timeout is hit.
            batch = self._loop.run_until_complete(
                asyncio.wait_for(anext_batch(), timeout.total_seconds())
            )
            return batch
        except StopAsyncIteration:
            # Suppress automatic exception chaining.
            raise StopIteration() from None
