"""Helpers to facilitate running on multiple workers.

Use these functions to start multiple processes.
"""
import multiprocessing as mp

import os

from bytewax import Executor


def start_local(
    executor: Executor,
    number_of_processes: int = 1,
    threads_per_process: int = 1,
    ctrlc=True,
):
    """Start a number of local workers

    Convenience method for starting a number of local worker processes
    using Python's multiprocessing package.

    Args:
        executor(:obj:`Executor`): The bytewax executor
        number_of_workers(int): Number of local process workers to start
        threads_per_worker(int): Number of threads per worker process
        ctrlc(bool): Configure a ctrlc handler for this worker
    """
    for i in range(number_of_processes):
        p = mp.Process(
            target=executor.build_and_run,
            args=(threads_per_process, i, number_of_processes, ctrlc),
        )
        p.start()


def start_kubernetes(
    executor: Executor,
    ctrlc=True,
):
    """Start a number of workers in a kubernetes cluster

    Convenience method for starting a number worker processes in Kubernetes
    using Python's multiprocessing package.

    Args:
        executor(:obj:`Executor`): The bytewax executor
        ctrlc(bool): Configure a ctrlc handler for this worker
    """

    threads_per_process = int(os.getenv("BYTEWAX_WORKERS_PER_PROCESS"))
    
    BYTEWAX_STATEFULSET_NAME = os.getenv("BYTEWAX_STATEFULSET_NAME")
    BYTEWAX_POD_NAME = os.getenv("BYTEWAX_POD_NAME")
    process = int(BYTEWAX_POD_NAME.replace(BYTEWAX_STATEFULSET_NAME + "-", ""))
    
    number_of_processes = int(os.getenv("BYTEWAX_REPLICAS"))

    BYTEWAX_HOSTFILE_PATH = os.getenv("BYTEWAX_HOSTFILE_PATH")
    hostfile = open(BYTEWAX_HOSTFILE_PATH, "r")
    addresses = hostfile.read().splitlines()
    hostfile.close()    

    p = mp.Process(
        target=executor.build_and_run,
        args=(threads_per_process, process, number_of_processes, ctrlc, addresses),
    )
    p.start()