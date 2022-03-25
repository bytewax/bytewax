"""Entry point functions to execute `bytewax.Dataflow`s.

"""
import asyncio
import concurrent.futures
import logging
import threading
import traceback
from collections import abc
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
from tempfile import TemporaryDirectory

from typing import Any, Callable, Iterable, Tuple

import dill

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
    >>> async def output_builder(worker_index, worker_count, epoch_items):
    ...     async for epoch, item in epoch_items:
    ...         print(epoch, item)
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

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out_buffer.append(epoch_item)

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
    >>> async def output_builder(worker_index, worker_count, epoch_items):
    ...     async for epoch, item in epoch_items:
    ...         print(epoch, item)
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
            # Note that these are not `asyncio.Future`s, so we use a
            # different waiting function.
            for future in concurrent.futures.as_completed(futures):
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


def _proc_main(
    flow_bytes,
    input_builder_bytes,
    output_builder_bytes,
    addresses,
    proc_id,
    worker_count_per_proc,
    proc_should_stop,
):
    """Make a little wrapper function that uses dill for pickling lambdas."""
    try:
        cluster_main(
            dill.loads(flow_bytes),
            dill.loads(input_builder_bytes),
            dill.loads(output_builder_bytes),
            addresses,
            proc_id,
            worker_count_per_proc,
            dill.loads(proc_should_stop),
        )
    # Handle exceptions that aren't pickle-able by wrapping original
    # exception into __cause__.
    except Exception as ex:
        raise RuntimeError(
            f"exception in process {proc_id}; see above for full traceback"
        ) from ex


async def spawn_cluster(
    flow: Dataflow,
    input_builder: Callable[[int, int], Iterable[Tuple[int, Any]]],
    output_builder: Callable[[int, int], Callable[[Tuple[int, Any]], None]],
    proc_count: int = 1,
    worker_count_per_proc: int = 1,
    cluster_should_stop: Callable[[], bool] = lambda: False
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
    >>> async def output_builder(worker_index, worker_count, epoch_items):
    ...     async for epoch, item in epoch_items:
    ...         return print(epoch, item)
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

        cluster_should_stop: Returns if this cluster should gracefully
            shutdown soon.

    """
    addresses = _gen_addresses(proc_count)

    manager = Manager()
    proc_raised_ex = manager.Event()

    def proc_should_stop():
        return cluster_should_stop() or proc_raised_ex.is_set()

    with ProcessPoolExecutor(max_workers=proc_count) as pool:
        pending_futures = [
            asyncio.wrap_future(
                pool.submit(
                    _proc_main,
                    dill.dumps(flow),
                    dill.dumps(input_builder),
                    dill.dumps(output_builder),
                    addresses,
                    proc_id,
                    worker_count_per_proc,
                    dill.dumps(proc_should_stop),
                )
            )
            for proc_id in range(proc_count)
        ]

        try:
            while len(pending_futures) > 0:
                completed_futures, pending_futures = await asyncio.wait(
                    pending_futures,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for future in completed_futures:
                    if future.exception():
                        raise future.exception()
        # Do this out here to also catch KeyboardInterrupt during
        # await.
        except:
            proc_raised_ex.set()
            raise


def _input_task(in_q, worker_count, inp):
    logging.debug("Input task started")
    for epoch_item in inp:
        msg = ("yield", epoch_item)
        msg_bytes = dill.dumps(msg)
        in_q.put(msg_bytes)
        logging.debug("Input sent %s", msg)

    logging.debug("Input iterator complete")
    # Input is complete, signal workers to shutdown.
    for i in range(worker_count):
        msg = ("return", None)
        msg_bytes = dill.dumps(msg)
        in_q.put(msg_bytes)
        logging.debug("Input sent %s", msg)
    logging.debug("Input task done")


def _output_iter(out_q, worker_count):
    logging.debug("Output task started")
    done_count = 0
    while done_count < worker_count:
        msg_bytes = out_q.get()
        msg = dill.loads(msg_bytes)
        logging.debug("Output got %s", msg)
        typ, data = msg
        if typ == "yield":
            epoch_item = data
            yield epoch_item
        elif typ == "done":
            done_count += 1
            logging.debug(
                "Output waiting for %s/%s workers", done_count, worker_count
            )
        else:
            raise ValueError("unknown output IPC type: {typ!r}")
    logging.debug("Output task done")


class _StopIterationShim(Exception):
    pass
    

def _step_output_iter(output_iter):
    try:
        return output_iter.__next__()
    except StopIteration as ex:
        raise _StopIterationShim() from ex


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
            communicating to subprocesses. Defautls to total number of
            worker threads in cluster * 2.

    Yields:

        `(epoch, item)` tuples seen by capture operators.

    """
    worker_count = proc_count * worker_count_per_proc
    if ipc_buffer_size is None:
        ipc_buffer_size = worker_count * 2
    manager = Manager()

    main_raised_ex = manager.Event()
    
    in_q = manager.Queue(ipc_buffer_size)

    def input_builder(worker_index, worker_count):
        logging.debug("Worker %s input starting", worker_index)
        while True:
            msg_bytes = in_q.get()
            msg = dill.loads(msg_bytes)
            logging.debug("Worker %s got %s", worker_index, msg)
            typ, data = msg
            if typ == "yield":
                epoch_item = data
                yield epoch_item
            elif typ == "return":
                return
            else:
                raise ValueError(f"unknown input IPC type: {typ!r}")
        logging.debug("Worker %s input done", worker_index)

    out_q = manager.Queue(ipc_buffer_size)

    async def output_builder(worker_index, worker_count, epoch_items):
        logging.debug("Worker %s output starting", worker_index)
        async for epoch_item in epoch_items:
            msg = ("yield", epoch_item)
            msg_bytes = dill.dumps(msg)
            out_q.put(msg_bytes)
            logging.debug("Worker %s sent %s", worker_index, msg)

        msg = ("done", None)
        msg_bytes = dill.dumps(msg)
        out_q.put(msg_bytes)
        logging.debug("Worker %s sent %s", worker_index, msg)
        logging.debug("Worker %s output done", worker_index)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Spawn before making threads. Threads and (possible) forks don't
    # mix.
    cluster_task = loop.create_task(
        spawn_cluster(
            flow,
            input_builder,
            output_builder,
            proc_count,
            worker_count_per_proc,
            main_raised_ex.is_set,
        ),
        name="cluster-task",
    )
    input_task = loop.create_task(
        asyncio.to_thread(
            _input_task,
            in_q,
            worker_count,
            inp,
        ),
        name="input-task",
    )
    output_iter = _output_iter(
        out_q,
        worker_count,
    )
    output_step_task = loop.create_task(
        asyncio.to_thread(_step_output_iter, output_iter),
        name="output-step-task",
    )

    pending_tasks = [cluster_task, input_task, output_step_task]
    try:
        while len(pending_tasks) > 0:
            # Will block until one of our tasks completes.
            for coro in asyncio.as_completed(pending_tasks):
                # This is like a combo of run-then-result, but we want to
                # handle exceptions differently per-task, so swallow
                # everything here and we'll re-raise them below.
                try:
                    loop.run_until_complete(coro)
                except _StopIterationShim:
                    pass
                # We just want to do things once one of our tasks is done.
                break

            pending_tasks = []

            if not cluster_task.done():
                pending_tasks.append(cluster_task)
            elif cluster_task.exception():
                raise cluster_task.exception()

            if not input_task.done():
                pending_tasks.append(input_task)
            elif input_task.exception():
                raise input_task.exception()

            if not output_step_task.done():
                pending_tasks.append(output_step_task)
            else:
                try:
                    yield output_step_task.result()

                    output_step_task = loop.create_task(
                        asyncio.to_thread(_step_output_iter, output_iter),
                        name="output-step-task",
                    )
                    pending_tasks.append(output_step_task)
                except _StopIterationShim:
                    pass
    except:
        main_raised_ex.set()
        raise
