"""Low-level input interfaces.

If you want pre-built connectors for various external systems, see
`bytewax.connectors`. That is also a rich source of examples.

Subclass the types here to implement input for your own custom source.

"""

import asyncio
import sys
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any, List, Optional

__all__ = [
    "AsyncBatcher",
    "DynamicInput",
    "Input",
    "PartitionedInput",
    "StatefulSource",
    "StatelessSource",
]


class Input(ABC):  # noqa: B024
    """Base class for all input types. Do not subclass this.

    If you want to implement a custom connector, instead subclass one
    of the specific input sub-types below in this module.

    """

    def __json__(self):
        """JSON representation of this.

        This is used by the Bytewax platform internally and should
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

    def next_awake(self) -> Optional[datetime]:
        """When to next attempt to get input items.

        `next_batch()` will not be called until the most recently returned
        time has past.

        This will be called upon initialization of the source and
        after `next_batch()`, but also possibly at other times. Multiple
        times are not stored; you must return the next awake time on
        every call, if any.

        If this returns `None`, `next_batch()` will be called
        immediately unless the previous batch had no items, in which
        case there is a 1 millisecond delay.

        Use this instead of `time.sleep` in `next_batch()`.

        Returns:
            Next awake time or `None` to indicate automatic behavior.

        """
        return None

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
        """Cleanup this source when the dataflow completes.

        This is not guaranteed to be called. It will only be called
        when the dataflow finishes on finite input. It will not be
        called during an abrupt or abort shutdown.

        """
        return


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
        """Build anew or resume an input partition.

        Will be called once per execution for each partition key on a
        worker that reported that partition was local in `list_parts`.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.

        Args:
            for_part:
                Which partition to build. Will always be one of the
                keys returned by `list_parts` on this worker.

            resume_state:
                State data containing where in the input stream this
                partition should be begin reading during this
                execution.

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

    def next_awake(self) -> Optional[datetime]:
        """When to next attempt to get input items.

        `next_batch()` will not be called until the most recently returned
        time has past.

        This will be called upon initialization of the source and
        after `next_batch()`, but also possibly at other times. Multiple
        times are not stored; you must return the next awake time on
        every call, if any.

        If this returns `None`, `next_batch()` will be called
        immediately unless the previous batch had no items, in which
        case there is a 1 millisecond delay.

        Use this instead of `time.sleep` in `next_batch()`.

        Returns:
            Next awake time or `None` to indicate automatic behavior.

        """
        return None

    def close(self) -> None:
        """Cleanup this source when the dataflow completes.

        This is not guaranteed to be called. It will only be called
        when the dataflow finishes on finite input. It will not be
        called during an abrupt or abort shutdown.

        """
        return


class DynamicInput(Input):
    """An input where all workers can read distinct items.

    Does not support storing any resume state. Thus these kind of
    inputs only naively can support at-most-once processing.

    The source must somehow support supplying disjoint data for each
    worker. If you re-read the same items on multiple workers, the
    dataflow will process these as duplicate items.

    """

    @abstractmethod
    def build(self, worker_index, worker_count) -> StatelessSource:
        """Build an input source for a worker.

        Will be called once on each worker.

        Args:
            worker_index:
                Index of this worker.
            worker_count:
                Total number of workers.

        Returns:
            Input source.

        """
        ...


class AsyncBatcher:
    """Poll an async iter synchronously with a time and length limit.

    This is great for allowing async input sources to play well with
    Bytewax's requirement that calling `next` on sources should never
    block.

    """

    def __init__(self, ait: AsyncIterator, loop=None):
        """Init.

        Args:
            ait:
                The underlying source async iterator of items.
            loop:
                Custom `asyncio` run loop to use, if any.
        """
        self._ait = ait

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
                    self._task = self._loop.create_task(self._ait.__anext__())

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
