"""Helpers to facilitate running on multiple workers.

Use these functions to start multiple processes.
"""
import multiprocessing as mp

from bytewax import Executor


def start_local(
    executor: Executor,
    number_of_processes: int = 1,
    threads_per_process: int = 1,
):
    """Start a number of local workers

    Convenience method for starting a number of local worker processes
    using Python's multiprocessing package.

    Args:
        executor(:obj:`Executor`): The bytewax executor
        number_of_workers(int): Number of local process workers to start
        threads_per_worker(int): Number of threads per worker process
    """
    for i in range(number_of_processes):
        p = mp.Process(
            target=executor.build_and_run,
            args=(threads_per_process, i, number_of_processes),
        )
        p.start()
