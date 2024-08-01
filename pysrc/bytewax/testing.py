"""Helper tools for testing dataflows."""

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union

from bytewax._bytewax import (
    cluster_main,
    run_main,
)
from bytewax.inputs import (
    AbortExecution,
    FixedPartitionedSource,
    StatefulSourcePartition,
    X,
)
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.run import (
    _create_arg_parser,
    _EnvDefault,
)
from typing_extensions import override

__all__ = [
    "TestingSink",
    "TestingSource",
    "TimeTestingGetter",
    "ffwd_iter",
    "poll_next_batch",
    "run_main",
    "cluster_main",
]


@dataclass
class TimeTestingGetter:
    """Wrapper to provide a modifyable system clock for unit tests."""

    now: datetime

    def advance(self, td: timedelta) -> None:
        """Advance the current time.

        :arg td: By this amount.

        """
        self.now += td

    def get(self) -> datetime:
        """Return the "current time".

        Use this if you need a getter.

        :returns: The "current time".

        """
        return self.now


def ffwd_iter(it: Iterator[Any], n: int) -> None:
    """Skip an iterator forward some number of items.

    :arg it: A stateful iterator to advance.

    :arg n: Number of items to skip from the current position.

    """
    # Taken from `consume`
    # https://docs.python.org/3/library/itertools.html#itertools-recipes
    # Apparently faster than a for loop.
    next(islice(it, n, n), None)


class _IterSourcePartition(StatefulSourcePartition[X, int]):
    def __init__(
        self,
        ib: Iterable[
            Union[
                X,
                "TestingSource.EOF",
                "TestingSource.ABORT",
                "TestingSource.PAUSE",
            ]
        ],
        batch_size: int,
        resume_state: Optional[int],
    ):
        self._start_idx = 0 if resume_state is None else resume_state
        self._batch_size = batch_size
        self._next_awake: Optional[datetime] = None
        self._it = iter(ib)
        # Resume to one after the last completed read index.
        ffwd_iter(self._it, self._start_idx)
        self._raise: Optional[Exception] = None

    @override
    def next_batch(self) -> List[X]:
        if self._raise is not None:
            raise self._raise
        if self._next_awake is not None:
            self._next_awake = None

        batch = []
        for item in self._it:
            if isinstance(item, TestingSource.EOF):
                self._raise = StopIteration()
                # Skip over this on continuation.
                self._start_idx += 1
                # Batch is done early.
                break
            elif isinstance(item, TestingSource.ABORT):
                if not item._triggered:
                    self._raise = AbortExecution()
                    # Don't trigger on next execution.
                    item._triggered = True
                    # Batch is done early.
                    break
            elif isinstance(item, TestingSource.PAUSE):
                now = datetime.now(tz=timezone.utc)
                self._next_awake = now + item.for_duration
                # Batch is done early.
                break
            else:
                batch.append(item)
                if len(batch) >= self._batch_size:
                    break

        # If the last item was a sentinel, then don't say EOF, let the
        # next batch raise the exception.
        if len(batch) > 0 or self._raise is not None or self._next_awake is not None:
            self._start_idx += len(batch)
            return batch
        else:
            raise StopIteration()

    @override
    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

    @override
    def snapshot(self) -> int:
        return self._start_idx


class TestingSource(FixedPartitionedSource[X, int]):
    """Produce input from a Python iterable.

    You only want to use this for unit testing.

    The iterable must be identical on all workers.

    There is no parallelism; only one worker will actually consume the
    iterable.

    Be careful using a generator as the iterable; if you fail and
    attempt to resume the dataflow without rebuilding it, the
    half-consumed generator will be re-used on recovery and early
    input will be lost so resume will see the correct data.

    """

    __test__ = False

    @dataclass
    class EOF:
        """Signal the input to EOF.

        The next execution will continue from the item after this.

        """

        pass

    # TODO: Find a way to use ABORT in a multi-worker scenario.
    # Something like a back channel that one has aborted.
    @dataclass
    class ABORT:
        """Abort the execution when the input processes this item.

        The next execution will resume from some item befor this one.

        Each abort will only trigger once. They'll be skipped on
        resume executions.

        You cannot use this in multi-worker executions because the
        other workers don't know when to stop.

        """

        _triggered: bool = False

    @dataclass
    class PAUSE:
        """Signal this input to not emit items for a duration."""

        for_duration: timedelta

    def __init__(self, ib: Iterable[Union[X, EOF, ABORT, PAUSE]], batch_size: int = 1):
        """Init.

        :arg ib: Iterable for input.

        :arg batch_size: Number of items from the iterable to emit in
            each batch. Defaults to 1.

        """
        self._ib = ib
        self._batch_size = batch_size

    @override
    def list_parts(self):
        return ["iterable"]

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _IterSourcePartition[X]:
        return _IterSourcePartition(self._ib, self._batch_size, resume_state)


class _ListSinkPartition(StatelessSinkPartition[X]):
    def __init__(self, ls: List[X]):
        self._ls = ls

    @override
    def write_batch(self, items: List[X]) -> None:
        self._ls += items


class TestingSink(DynamicSink[X]):
    """Append each output item to a list.

    You only want to use this for unit testing.

    Can support at-least-once processing. The list is not cleared
    between executions.

    """

    __test__ = False

    def __init__(self, ls: List[X]):
        """Init.

        :arg ls: List to append to.

        """
        self._ls = ls

    @override
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _ListSinkPartition[X]:
        return _ListSinkPartition(self._ls)


def poll_next_batch(part, timeout=timedelta(seconds=5)):
    """Repeatedly poll a partition until it returns a batch.

    You'll want to use this in unit tests of partitions when there's
    some non-determinism in how items are read.

    This is a busy-loop.

    :arg part: To call `next_batch` on.

    :arg timeout: How long to continuously poll for.

    :returns: The next batch found.

    :raises TimeoutError: If no batch was returned within the timeout.

    """
    batch = []
    start = datetime.now(timezone.utc)
    while len(batch) <= 0:
        now = datetime.now(timezone.utc)
        if now - start > timeout:
            raise TimeoutError()
        batch = part.next_batch()
    return batch


def _unparse_args(args: Dict[str, Any]) -> Iterable[str]:
    for key, val in args.items():
        if val is not None:
            flag_key = key.replace("_", "-")
            yield f"--{flag_key}"
            if isinstance(val, timedelta):
                yield str(int(val.total_seconds()))
            else:
                yield str(val)


async def _create_wait_and_check(argv: List[str]) -> None:
    try:
        proc = await asyncio.create_subprocess_exec(*argv)
        await proc.wait()
    except asyncio.CancelledError:
        proc.kill()
        raise

    if proc.returncode != 0:
        msg = f"subprocess {argv!r} did not exit cleanly"
        raise RuntimeError(msg)


async def _testing_cli_main(
    import_str: str,
    processes: int,
    other_args: Dict[str, Any],
) -> None:
    unparsed_args = list(_unparse_args(other_args))

    addresses_str = ";".join(
        f"localhost:{2101 + proc_id}" for proc_id in range(processes)
    )

    argvs = [
        [
            sys.executable,
            "-m",
            "bytewax.run",
            import_str,
            "-i",
            str(proc_id),
            "-a",
            addresses_str,
        ]
        + unparsed_args
        for proc_id in range(processes)
    ]
    tasks = [asyncio.create_task(_create_wait_and_check(argv)) for argv in argvs]
    # Wish we could use TaskGroup, but thats 3.11+
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()


def _parse_args():
    parser = _create_arg_parser()

    # Add scaling arguments for the testing namespace
    scaling = parser.add_argument_group(
        "Scaling",
        "This testing entrypoint supports using '-p' to spawn multiple "
        "processes, and '-w' to run multiple workers within a process.",
    )
    scaling.add_argument(
        "-w",
        "--workers-per-process",
        type=int,
        help="Number of workers for each process; defaults to 1",
        default=1,
        action=_EnvDefault,
        envvar="BYTEWAX_WORKERS_PER_PROCESS",
    )
    scaling.add_argument(
        "-p",
        "--processes",
        type=int,
        help="Number of separate processes to run; defaults to 1",
        default=1,
        action=_EnvDefault,
        envvar="BYTEWAX_PROCESSES",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = vars(_parse_args())

    import_str = args.pop("import_str")
    processes = args.pop("processes")

    asyncio.run(_testing_cli_main(import_str, processes, args))
