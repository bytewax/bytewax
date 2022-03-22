"""Entry point functions to execute `bytewax.Dataflow`s.

"""
import asyncio
import threading
import time
from collections import abc, deque
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from queue import Empty, Full
from multiprocessing import Manager
from tempfile import TemporaryDirectory
import traceback

from typing import Any, Callable, Iterable, Tuple

import dill
import pynng

from .bytewax import Dataflow, WorkerCoro, WorkerBuilder


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
        proc_raised_ex,
):
    """Make a little wrapper function that uses dill for pickling lambdas.

    """
    cluster_main(
        dill.loads(flow_bytes),
        dill.loads(input_builder_bytes),
        dill.loads(output_builder_bytes),
        addresses,
        proc_id,
        worker_count_per_proc,
        proc_raised_ex.is_set,
    )


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

    with ProcessPoolExecutor(max_workers=proc_count) as pool:
        pending_futures = [
            asyncio.wrap_future(pool.submit(
                _proc_main,
                dill.dumps(flow),
                dill.dumps(input_builder),
                dill.dumps(output_builder),
                addresses,
                proc_id,
                worker_count_per_proc,
                proc_raised_ex,
            )) for proc_id in range(proc_count)
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


async def _push_input(input_addr, worker_count, inp):
    with pynng.Push0(listen=input_addr) as socket:
        for epoch_item in inp:
            msg = ("yield", epoch_item)
            print(f"Main sent {msg}")
            msg_bytes = dill.dumps(msg)
            await socket.asend(msg_bytes)
            
        # Input is complete, signal workers to shutdown.
        print("Main input complete")
        for i in range(worker_count):
            msg = ("return", None)
            print(f"Main sent {msg}")
            msg_bytes = dill.dumps(msg)
            await socket.asend(msg_bytes)


async def _pull_output(output_addr, worker_count):
    with pynng.Pull0(listen=output_addr) as socket:
        done_count = 0
        while done_count < worker_count:
            msg_bytes = await socket.arecv()
            msg = dill.loads(msg_bytes)
            print(f"Main got {msg}")
            typ, data = msg
            if typ == "yield":
                epoch_item = data
                yield epoch_item
            elif typ == "done":
                done_count += 1
            else:
                raise ValueError("unknown output IPC type: {typ!r}")
            print(f"Main done count {done_count}")
        print("Main output complete")

            
def run_cluster(
    flow: Dataflow,
    inp: Iterable[Tuple[int, Any]],
    proc_count: int = 1,
    worker_count_per_proc: int = 1,
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

    Yields:

        `(epoch, item)` tuples seen by capture operators.

    """
    worker_count = proc_count * worker_count_per_proc

    with TemporaryDirectory() as ipc_dir:
        input_addr = f"ipc://{ipc_dir}/input"
        output_addr = f"ipc://{ipc_dir}/output"

        def input_builder(worker_index, worker_count):
            # We're using pynng because there's no version of
            # `multiprocessing.Queue` that is async aware, and doesn't
            # use threads.
            with pynng.Pull0(dial=input_addr) as socket:
                while True:
                    msg_bytes = socket.recv()
                    msg = dill.loads(msg_bytes)
                    print(f"Worker {worker_index} got {msg}")
                    typ, data = msg
                    if typ == "yield":
                        epoch_item = data
                        yield epoch_item
                    elif typ == "return":
                        return
                    else:
                        raise ValueError(f"unknown input IPC type: {typ!r}")

        def output_builder(worker_index, worker_count):
            socket = pynng.Push0(dial=output_addr)
            def out_put(epoch_item):
                try:
                    msg = ("yield", epoch_item)
                    print(f"Worker {worker_index} sent {msg}")
                    msg_bytes = dill.dumps(msg)
                    socket.send(msg_bytes)
                
                    # TODO: How do we signal output is done from this worker
                    # and close the socket? Use an iterator instead of a
                    # callback?
                    msg = ("done", None)
                    print(f"Worker {worker_index} sent {msg}")
                    msg_bytes = dill.dumps(msg)
                    socket.send(msg_bytes)
                    socket.close()
                except Exception as ex:
                    traceback.print_exc()
                    print(f"POOP{ex!r}POOP")
                    raise
            return out_put

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cluster_task = loop.create_task(spawn_cluster(
            flow,
            input_builder,
            output_builder,
            proc_count,
            worker_count_per_proc,
        ))
        input_task = loop.create_task(_push_input(
            input_addr,
            worker_count,
            inp,
        ))
        output_aiter = _pull_output(
            output_addr,
            worker_count,
        )
        output_step_task = loop.create_task(output_aiter.__anext__())

        pending_tasks = [cluster_task, input_task, output_step_task]
        while len(pending_tasks) > 0:
            # Will block until one of our tasks completes.
            print(pending_tasks)
            for coro in asyncio.as_completed(pending_tasks):
                loop.run_until_complete(coro)
                break
            pending_tasks = []
            
            if cluster_task.done():
                if cluster_task.exception():
                    raise cluster_task.exception()
            else:
                pending_tasks.append(cluster_task)
                
            if input_task.done():
                if input_task.exception():
                    raise input_task.exception()
            else:
                pending_tasks.append(input_task)
                
            if output_step_task.done():
                try:
                    yield output_step_task.result()

                    output_step_task = loop.create_task(output_aiter.__anext__())
                    pending_tasks.append(output_step_task)
                except StopAsyncIteration:
                    pass
            else:
                pending_tasks.append(output_step_task)    
