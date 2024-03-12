"""Low-level input interfaces and input helpers.

If you want pre-built connectors for various external systems, see
{py:obj}`bytewax.connectors`.

"""

import asyncio
import queue
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import (
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
)

from typing_extensions import AsyncIterable, override

from bytewax._bytewax import AbortExecution

__all__ = [
    "AbortExecution",
    "DynamicSource",
    "FixedPartitionedSource",
    "S",
    "SimplePollingSource",
    "Source",
    "StatefulSourcePartition",
    "StatelessSourcePartition",
    "X",
    "batch",
    "batch_async",
    "batch_getter",
    "batch_getter_ex",
]

X = TypeVar("X")
"""Type emitted by a {py:obj}`Source`."""


S = TypeVar("S")
"""Type of state snapshots."""


class Source(ABC, Generic[X]):  # noqa: B024
    """A location to read input items from.

    Base class for all input sources. Do not subclass this.

    If you want to implement a custom connector, instead subclass one
    of the specific source sub-types below in this module.

    """

    pass


class StatefulSourcePartition(ABC, Generic[X, S]):
    """Input partition that maintains state of its position."""

    @abstractmethod
    def next_batch(self) -> Iterable[X]:
        """Attempt to get the next batch of input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but returning an empty list if there are no
        items to emit yet.

        :returns: Items immediately ready. May be empty if no new
            items.

        :raises StopIteration: When the source is complete.

        """
        ...

    def next_awake(self) -> Optional[datetime]:
        """When to next attempt to get input items.

        {py:obj}`next_batch` will not be called until the most
        recently returned time has past.

        This will be called upon initialization of the source and
        after {py:obj}`next_batch`, but also possibly at other times.
        Multiple times are not stored; you must return the next awake
        time on every call, if any.

        If this returns `None`, {py:obj}`next_batch` will be called
        immediately unless the previous batch had no items, in which
        case there is a 1 millisecond delay.

        Use this instead of {py:obj}`time.sleep` in
        {py:obj}`next_batch`.

        :returns: Next awake time or `None` to indicate automatic
            behavior.

        """
        return None

    @abstractmethod
    def snapshot(self) -> S:
        """Snapshot the position of the next read of this partition.

        This will be returned to you via the `resume_state` parameter
        of your input builder.

        Be careful of "off by one" errors in resume state. This should
        return a state that, when built into a partition, resumes
        reading _after the last read item_, not any of the the same
        items that {py:obj}`next_batch` last returned.

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


class FixedPartitionedSource(Source[X], Generic[X, S]):
    """An input source with a fixed number of independent partitions.

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

        :returns: Local partition keys.

        """
        ...

    @abstractmethod
    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[S],
    ) -> StatefulSourcePartition[X, S]:
        """Build anew or resume an input partition.

        Will be called once per execution for each partition key on a
        worker that reported that partition was local in
        {py:obj}`list_parts`.

        Do not pre-build state about a partition in the
        constructor. All state must be derived from `resume_state` for
        recovery to work properly.


        :arg step_id: The step_id of the input operator.

        :arg for_part: Which partition to build. Will always be one of
            the keys returned by {py:obj}`list_parts` on this worker.

        :arg resume_state: State data containing where in the input
            stream this partition should be begin reading during this
            execution.

        :returns: The built partition.

        """
        ...


class StatelessSourcePartition(ABC, Generic[X]):
    """Input partition that is stateless."""

    @abstractmethod
    def next_batch(self) -> Iterable[X]:
        """Attempt to get the next batch of input items.

        This must participate in a kind of cooperative multi-tasking,
        never blocking but yielding an empty list if there are no new
        items yet.

        :returns: Items immediately ready. May be empty if no new
            items.

        :raises StopIteration: When the source is complete.

        """
        ...

    def next_awake(self) -> Optional[datetime]:
        """When to next attempt to get input items.

        {py:obj}`next_batch` will not be called until the most
        recently returned time has past.

        This will be called upon initialization of the source and
        after {py:obj}`next_batch`, but also possibly at other times.
        Multiple times are not stored; you must return the next awake
        time on every call, if any.

        If this returns `None`, {py:obj}`next_batch` will be called
        immediately unless the previous batch had no items, in which
        case there is a 1 millisecond delay.

        Use this instead of {py:obj}`time.sleep` in
        {py:obj}`next_batch`.

        :returns: Next awake time or `None` to indicate automatic
            behavior.

        """
        return None

    def close(self) -> None:
        """Cleanup this partition when the dataflow completes.

        This is not guaranteed to be called. It will only be called
        when the dataflow finishes on finite input. It will not be
        called during an abrupt or abort shutdown.

        """
        return


class DynamicSource(Source[X]):
    """An input source where all workers can read distinct items.

    Does not support storing any resume state. Thus these kind of
    sources only naively can support at-most-once processing.

    The source must somehow support supplying disjoint data for each
    worker. If you re-read the same items on multiple workers, the
    dataflow will process these as duplicate items.

    """

    @abstractmethod
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> StatelessSourcePartition[X]:
        """Build an input source for a worker.

        Will be called once on each worker.

        :arg step_id: The step_id of the input operator.

        :arg worker_index: Index of this worker. Workers are zero-indexed.

        :arg worker_count: Total number of workers.

        :returns: The built partition.

        """
        ...


class _SimplePollingPartition(StatefulSourcePartition[X, None]):
    def __init__(
        self,
        now: datetime,
        interval: timedelta,
        align_to: Optional[datetime],
        getter: Callable[[], X],
    ):
        self._interval = interval
        self._getter = getter

        if align_to is not None:
            # Hell yeah timedelta implements remainder.
            since_last_awake = (now - align_to) % interval
            if since_last_awake > timedelta(seconds=0):
                until_next_awake = interval - since_last_awake
            else:
                # If now is exactly on the align_to mark (remainder is
                # 0), don't wait a whole interval; activate
                # immediately.
                until_next_awake = timedelta(seconds=0)
            self._next_awake = now + until_next_awake
        else:
            self._next_awake = now

    @override
    def next_batch(self) -> List[X]:
        try:
            item = self._getter()
            self._next_awake += self._interval
            if item is None:
                return []
            return [item]
        except SimplePollingSource.Retry as ex:
            self._next_awake += ex.timeout
            return []

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

    @override
    def snapshot(self) -> None:
        return None


class SimplePollingSource(FixedPartitionedSource[X, None]):
    """Calls a user defined function at a regular interval.

    ```python
    >>> class URLSource(SimplePollingSource):
    ...     def __init__(self):
    ...         super(interval=timedelta(seconds=10))
    ...
    ...     def next_item(self):
    ...         res = requests.get("https://example.com")
    ...         if not res.ok:
    ...             raise SimplePollingSource.Retry(timedelta(seconds=1))
    ...         return res.text
    ```

    There is no parallelism; only one worker will poll this source.

    Does not support storing any resume state. Thus these kind of
    sources only naively can support at-most-once processing.

    This is best for low-throughput polling on the order of seconds to
    hours.

    If you need a high-throughput source, or custom retry or timing,
    avoid this. Instead create a source using one of the other
    {py:obj}`Source` subclasses where you can have increased
    paralellism, batching, and finer control over timing.

    """

    @dataclass
    class Retry(Exception):
        """Raise this to try to get items before the usual interval.

        :arg timeout: How long to wait before calling
            {py:obj}`SimplePollingSource.next_item` again.

        """

        timeout: timedelta

    def __init__(self, interval: timedelta, align_to: Optional[datetime] = None):
        """Init.

        :arg interval: The interval between calling
            {py:obj}`next_item`.

        :arg align_to: Align awake times to the given datetime. Defaults
            to now.

        """
        self._interval = interval
        self._align_to = align_to

    @override
    def list_parts(self) -> List[str]:
        return ["singleton"]

    @override
    def build_part(
        self, _step_id: str, for_part: str, resume_state: Optional[None]
    ) -> _SimplePollingPartition[X]:
        now = datetime.now(timezone.utc)
        return _SimplePollingPartition(
            now, self._interval, self._align_to, self.next_item
        )

    @abstractmethod
    def next_item(self) -> X:
        """Override with custom logic to poll your source.

        :returns: Next item to emit into the dataflow. If `None`, no
            item is emitted.

        :raises Retry: Raise if you can't fetch items and would like
            to call this function sooner than the usual interval.

        """
        ...


def batch(ib: Iterable[X], batch_size: int) -> Iterator[List[X]]:
    """Batch an iterable.

    Use this to easily generate batches of items for a partitions's
    `next_batch` method.

    :arg ib: The underlying source iterable of items.

    :arg batch_size: Maximum number of items to yield in a batch.

    :yields: The next gathered batch of items.

    """
    # Ensure that we have the stateful iterator of the source.
    it = iter(ib)
    while True:
        batch = list(islice(it, batch_size))
        if len(batch) <= 0:
            return
        yield batch


def batch_getter(
    getter: Callable[[], X], batch_size: int, yield_on: Optional[X] = None
) -> Iterator[List[X]]:
    """Batch from a getter function that might not return an item.

     Use this to easily generate batches of items for a partitions's
    `next_batch` method.

    :arg getter: Function to call to get the next item. Should raise
        {py:obj}`StopIteration` on EOF.

    :arg batch_size: Maximum number of items to yield in a batch.

    :arg yield_on: Sentinel value that indicates that there are no
            more items yet, and to return the current batch. Defaults
            to `None`.

    :yields: The next gathered batch of items.

    """
    while True:
        batch = []
        while len(batch) < batch_size:
            try:
                item = getter()
                if item != yield_on:
                    batch.append(item)
                else:
                    break
            except StopIteration:
                yield batch
                return
        yield batch


def batch_getter_ex(
    getter: Callable[[], X], batch_size: int, yield_ex: Type[Exception] = queue.Empty
) -> Iterator[List[X]]:
    """Batch from a getter function that raises on no items yet.

     Use this to easily generate batches of items for a partitions's
    `next_batch` method.

    :arg getter: Function to call to get the next item. Should raise
        {py:obj}`StopIteration` on EOF.

    :arg batch_size: Maximum number of items to return in a batch.

    :arg yield_ex: Exception raised by `getter` that indicates that
        there are no more items yet, and to return the current batch.
        Defaults to {py:obj}`queue.Empty`.

    :yields: The next gathered batch of items.

    """
    while True:
        batch = []
        while len(batch) < batch_size:
            try:
                item = getter()
                batch.append(item)
            except yield_ex:
                break
            except StopIteration:
                yield batch
                return
        yield batch


def batch_async(
    aib: AsyncIterable[X],
    timeout: timedelta,
    batch_size: int,
    loop=None,
) -> Iterator[List[X]]:
    """Batch an async iterable synchronously up to a timeout.

    This allows using an async iterator as an input source. The
    `next_batch` method on a partition must never block, this allows
    running an async iterator up to a timeout so that you correctly
    cooperatively multitask with the rest of the dataflow.

    :arg aib: The underlying source async iterable of items.

    :arg timeout: Duration of time to repeatedly poll the source async
        iterator for items.

    :arg batch_size: Maximum number of items to yield in a batch, even
        if the timeout has not been hit.

    :arg loop: Custom `asyncio` run loop to use, if any.

    :yields: The next gathered batch of items.

        This function will take up to `timeout` time to yield, or
        will return a list with length up to `max_len`.

    """
    # Ensure that we have the stateful iterator of the source.
    ait = aib.__aiter__()

    loop = loop if loop is not None else asyncio.new_event_loop()
    task = None

    async def anext_batch():
        nonlocal task

        batch = []
        # Only try to gather this many items.
        for _ in range(batch_size):
            if task is None:

                async def coro():
                    return await ait.__anext__()

                task = loop.create_task(coro())

            try:
                # Prevent the `wait_for` cancellation from
                # stopping the `__anext__` task; usually all
                # sub-tasks are cancelled too. It'll be re-used in
                # the next batch.
                next_item = await asyncio.shield(task)
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
            task = None
        return batch

    while True:
        try:
            # `wait_for` will raise `CancelledError` at the internal
            # await point in `anext_batch` if the timeout is hit.
            batch = loop.run_until_complete(
                asyncio.wait_for(anext_batch(), timeout.total_seconds())
            )
            yield batch
        except StopAsyncIteration:
            return
