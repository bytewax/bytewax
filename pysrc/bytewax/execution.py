"""Entry point functions to execute `bytewax.Dataflow`s.

"""
import asyncio
import threading
import time
from collections import abc, deque
from concurrent.futures import as_completed, ThreadPoolExecutor
from queue import Empty, Full

from typing import Any, Callable, Iterable, Tuple

from multiprocess import Manager, Pool, TimeoutError

from .bytewax import Dataflow, WorkerBuilder, WorkerCoro


def __skip_doctest_on_win_gha():
    import os, pytest

    if os.name == "nt" and os.environ.get("GITHUB_ACTION") is not None:
        pytest.skip("Hangs in Windows GitHub Actions")


def __fix_pickling_in_doctest():
    import dill.settings

    dill.settings["recurse"] = True


class WorkerCoroWrapper(abc.Coroutine, abc.Iterable):
    """We can't subclass Python-native classes in PyO3 yet.

    This wrapper class implements abc.Coroutine and abc.Iterable since
    we need that for asyncio compatibility.

    https://github.com/PyO3/pyo3/issues/991

    """

    def __init__(self, wrap: WorkerCoro):
        self.wrap = wrap

    def send(self, value):
        return self.wrap.send(value)

    def close(self):
        return self.wrap.close()

    def throw(self, type, value, traceback):
        return self.wrap.throw(type, value, traceback)

    def __await__(self):
        return self.wrap.__await__()

    def __next__(self):
        return self.wrap.__next__()

    def __iter__(self):
        return self.wrap.__iter__()


async def run_main(
    flow: Dataflow,
    input_builder: Callable[[int, int], Iterable[Tuple[int, Any]]],
    output_builder: Callable[[int, int], Callable[[Tuple[int, Any]], None]],
    should_stop: Callable[[], bool] = lambda: False,
) -> None:
    """Execute a dataflow in the current thread.

    You'd commonly use this for prototyping custom input and output
    builders with a single worker before using them in a cluster
    setting.

    Returns after execution is complete. Use with `asyncio.run()` if
    you want to block until the dataflow is complete.

    >>> import asyncio
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> def input_builder(worker_index, worker_count):
    ...     return enumerate(range(3))
    >>> def output_builder(worker_index, worker_count):
    ...     return print
    >>> asyncio.run(run_main(flow, input_builder, output_builder))  # doctest: +ELLIPSIS
    (...)

    See `bytewax.run()` for a convenience method to not need to worry
    about input or output builders.

    Args:

        flow: Dataflow to run.

        input_builder: Returns input that each worker thread should
            process.

        output_builder: Returns a callback function for each worker
            thread, called with `(epoch, item)` whenever and item
            passes by a capture operator on this process.

        should_stop: Returns if this worker should gracefully
            shutdown soon.

    """
    return await WorkerCoroWrapper(
        WorkerBuilder.sync().build(flow, input_builder, output_builder, should_stop)
    )


def _step_coro(coro: abc.Coroutine) -> bool:
    """Step a coroutine that has paused but is not waiting on any data to
    be sent. Returns if the coroutine is complete.

    This'll only work for the way `WorkerCoro` and `spawn_cluster()`
    are built.

    """
    try:
        coro.send(None)
    except StopIteration:
        coro.close()
        return False
    return True


def run(flow: Dataflow, inp: Iterable[Tuple[int, Any]]) -> Iterable[Tuple[int, Any]]:
    """Pass data through a dataflow running in the current thread.

    Handles distributing input and collecting output. You'd commonly
    use this for tests or prototyping in notebooks.

    Lazy: only computes new items when needed. Cast to `list` if you
    need complete execution immediately, but that won't work for
    infinite input iterators.

    >>> flow = Dataflow()
    >>> flow.map(str.upper)
    >>> flow.capture()
    >>> out = run(flow, [(0, "a"), (1, "b"), (2, "c")])
    >>> sorted(out)
    [(0, 'A'), (1, 'B'), (2, 'C')]

    See `bytewax.run_cluster()` for a multi-worker version of this
    function.

    Args:

        flow: Dataflow to run.

        inp: Input data.

    Yields:

        `(epoch, item)` tuples seen by capture operators.

    """

    def input_builder(worker_index, worker_count):
        return inp

    out_buffer = []

    def output_builder(worker_index, worker_count):
        return out_buffer.append

    coro = run_main(flow, input_builder, output_builder)

    work_remains = True
    while work_remains:
        work_remains = _step_coro(coro)

        for epoch, item in out_buffer:
            yield epoch, item
        out_buffer.clear()


def cluster_main(
    flow: Dataflow,
    input_builder: Callable[[int, int], Iterable[Tuple[int, Any]]],
    output_builder: Callable[[int, int], Callable[[Tuple[int, Any]], None]],
    addresses: Iterable[str],
    proc_id: int,
    worker_count_per_proc: int = 1,
    proc_should_stop: Callable[[], bool] = lambda: False,
) -> None:
    """Execute a dataflow in the current process as part of a cluster.

    You have to coordinate starting up all the processes in the
    cluster and ensuring they each are assigned a unique ID and know
    the addresses of other processes. You'd commonly use this for
    starting processes as part of a Kubernetes cluster.

    Blocks until execution is complete.

    >>> flow = Dataflow()
    >>> flow.capture()
    >>> def input_builder(worker_index, worker_count):
    ...     return enumerate(range(3))
    >>> def output_builder(worker_index, worker_count):
    ...     return print
    >>> cluster_main(
    ...     flow,
    ...     input_builder,
    ...     output_builder,
    ...     addresses=["localhost:2101"],
    ...     proc_id=0,
    ... )  # doctest: +ELLIPSIS
    (...)

    See `bytewax.run_cluster()` for a convenience method to pass data
    through a dataflow for notebook development.

    See `bytewax.spawn_cluster()` for starting a simple cluster
    locally on one machine.

    Args:

        flow: Dataflow to run.

        input_builder: Returns input that each worker thread should
            process.

        output_builder: Returns a callback function for each worker
            thread, called with `(epoch, item)` whenever and item
            passes by a capture operator on this process.

        addresses: List of host/port addresses for all processes in
            this cluster (including this one).

        proc_id: Index of this process in cluster; starts from 0.

        worker_count_per_proc: Number of worker threads to start on
            this process.

        proc_should_stop: Returns if workers in this process should
            gracefully shutdown soon.

    """
    builders, comms_threads = WorkerBuilder.cluster(
        addresses, proc_id, worker_count_per_proc
    )

    worker_raised_ex = threading.Event()

    def worker_should_stop():
        return proc_should_stop() or worker_raised_ex.is_set()

    def worker_main(builder):
        coro = builder.build(flow, input_builder, output_builder, worker_should_stop)
        asyncio.run(WorkerCoroWrapper(coro))

    with ThreadPoolExecutor(max_workers=worker_count_per_proc) as pool:
        futures = [pool.submit(worker_main, builder) for builder in builders]

        try:
            for future in as_completed(futures):
                if future.exception():
                    raise future.exception()
        # Do this out here to also catch KeyboardInterrupt during
        # as_completed() blocking.
        except:
            worker_raised_ex.set()
            raise

    comms_threads.join()


def _gen_addresses(proc_count: int) -> Iterable[str]:
    return [f"localhost:{proc_id + 2101}" for proc_id in range(proc_count)]


async def spawn_cluster(
    flow: Dataflow,
    input_builder: Callable[[int, int], Iterable[Tuple[int, Any]]],
    output_builder: Callable[[int, int], Callable[[Tuple[int, Any]], None]],
    proc_count: int = 1,
    worker_count_per_proc: int = 1,
) -> None:
    """Execute a dataflow as a cluster of processes on this machine.

    Returns after execution is complete. Use with `asyncio.run()` if
    you want to block until the dataflow is complete.

    Starts up cluster processes for you and handles connecting them
    together. You'd commonly use this for notebook analysis that needs
    parallelism and higher throughput, or simple stand-alone demo
    programs.

    >>> import pytest; pytest.skip("Figure out why doctest is pickling output.")
    >>> __skip_doctest_on_win_gha()
    >>> import asyncio
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> def input_builder(worker_index, worker_count):
    ...     return enumerate(range(3))
    >>> def output_builder(worker_index, worker_count):
    ...     return print
    >>> asyncio.run(spawn_cluster(
    ...     flow,
    ...     input_builder,
    ...     output_builder,
    ...     proc_count=2,
    ... ))  # doctest: +ELLIPSIS
    (...)

    See `bytewax.run_cluster()` for a convenience method to pass data
    through a dataflow for notebook development.

    See `bytewax.cluster_main()` for starting one process in a cluster
    in a distributed situation.

    Args:

        flow: Dataflow to run.

        input_builder: Returns input that each worker thread should
            process.

        output_builder: Returns a callback function for each worker
            thread, called with `(epoch, item)` whenever and item
            passes by a capture operator on this process.

        proc_count: Number of processes to start.

        worker_count_per_proc: Number of worker threads to start on
            each process.

    """
    addresses = _gen_addresses(proc_count)

    manager = Manager()
    proc_raised_ex = manager.Event()

    def proc_main(proc_id):
        cluster_main(
            flow,
            input_builder,
            output_builder,
            addresses,
            proc_id,
            worker_count_per_proc,
            proc_raised_ex.is_set,
        )

    # Can't use concurrent.ProcessPoolExecutor because it doesn't
    # support dill pickling.
    with Pool(processes=proc_count) as pool:
        results = [
            pool.apply_async(proc_main, (proc_id,)) for proc_id in range(proc_count)
        ]
        pool.close()

        while len(results) > 0:
            not_ready = []
            for result in results:
                try:
                    # Will re-raise exceptions from subprocesses.
                    result.get(0)
                except TimeoutError:
                    not_ready.append(result)
                except:
                    proc_raised_ex.set()
                    raise
            results = not_ready
            await asyncio.sleep(0)
        pool.join()


def run_cluster(
    flow: Dataflow,
    inp: Iterable[Tuple[int, Any]],
    proc_count: int = 1,
    worker_count_per_proc: int = 1,
    ipc_buffer_size: int = None,
) -> Iterable[Tuple[int, Any]]:
    """Pass data through a dataflow running as a cluster of processes on
    this machine.

    Starts up cluster processes for you, handles connecting them
    together, distributing input, and collecting output. You'd
    commonly use this for notebook analysis that needs parallelism and
    higher throughput, or simple stand-alone demo programs.

    >>> __skip_doctest_on_win_gha()
    >>> flow = Dataflow()
    >>> flow.map(str.upper)
    >>> flow.capture()
    >>> out = run_cluster(flow, [(0, "a"), (1, "b"), (2, "c")], proc_count=2)
    >>> sorted(out)
    [(0, 'A'), (1, 'B'), (2, 'C')]

    See `bytewax.run()` for an easier to debug, in-thread version of
    this function.

    See `bytewax.spawn_cluster()` for starting a cluster on this
    machine with full control over inputs and outputs.

    See `bytewax.cluster_main()` for starting one process in a cluster
    in a distributed situation.

    Args:

        flow: Dataflow to run.

        inp: Input data. Will be reifyied to a list before sending to
            processes. Will be partitioned between workers for you.

        proc_count: Number of processes to start.

        worker_count_per_proc: Number of worker threads to start on
            each process.

        ipc_buffer_size: How big to make the IO queues when
            communicating to subprocesses. Defaults to total number of
            worker threads in cluster * 2.

    Yields:

        `(epoch, item)` tuples seen by capture operators.

    """
    worker_count = proc_count * worker_count_per_proc
    if ipc_buffer_size is None:
        ipc_buffer_size = worker_count * 2
    manager = Manager()

    in_q = manager.Queue(ipc_buffer_size)

    def input_builder(worker_index, worker_count):
        def in_get():
            while True:
                msg, data = in_q.get()
                if msg == "yield":
                    yield data
                elif msg == "done":
                    return

        return in_get()

    out_q = manager.Queue(ipc_buffer_size)

    def output_builder(worker_index, worker_count):
        def out_put(epoch_item):
            out_q.put(epoch_item)

        return out_put

    inp_iter = iter(inp)
    coro = spawn_cluster(
        flow, input_builder, output_builder, proc_count, worker_count_per_proc
    )

    work_remains = True
    input_remains = True
    # Use an input buffer in this proc to store items we've
    # irreversibly popped off the input iterator in case the worker
    # input queue is full.
    input_buffer = deque()
    while work_remains or input_remains or len(input_buffer) > 0 or not out_q.empty():
        # 1. Check on workers.
        if work_remains:
            work_remains = _step_coro(coro)
            # If input / output queues are full / empty, this'll be a
            # hot busy loop so slow down to wait for IO to do.
            time.sleep(0.001)

        # 2. Read and buffer some input.
        while input_remains and len(input_buffer) < ipc_buffer_size:
            try:
                msg = ("yield", next(inp_iter))
                input_buffer.appendleft(msg)
            except StopIteration:
                input_remains = False
                # If the input is complete, enqueue messages to
                # workers to stop work.
                msg = ("done", None)
                for _ in range(worker_count):
                    input_buffer.appendleft(msg)

        # 3. Send as much input as we can to workers.
        while len(input_buffer) > 0:
            try:
                msg = input_buffer.pop()
                in_q.put(msg, False)
            except Full:
                input_buffer.append(msg)
                break

        # 4. Get as much output as we can from workers.
        while not out_q.empty():
            try:
                yield out_q.get(False)
            except Empty:
                break
