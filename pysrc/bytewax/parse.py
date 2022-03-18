import os
from argparse import ArgumentParser
from typing import Any, Dict, Iterable, List, Optional, Tuple


def __skip_doctest_on_win_gha():
    import os, pytest

    if os.name == "nt" and os.environ.get("GITHUB_ACTION"):
        pytest.skip("Hangs in Windows GitHub Actions")


def cluster_args(args: Iterable[str] = None) -> Dict[str, Any]:
    """Parse command line arguments to generate arguments for
    `run_cluster()`

    See documentation for `run_cluster()` for semantics of these
    variables.

    >>> __skip_doctest_on_win_gha()
    >>> from bytewax import Dataflow, run_cluster
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> args = "-w2 -n2".split()
    >>> out = run_cluster(flow, enumerate(range(3)), **cluster_args(args))
    >>> sorted(out)
    [(0, 0), (1, 1), (2, 2)]

    Args:
        args: List of arguments to parse. Defaults to `sys.argv`.
    Returns: kwargs to pass to `run_cluster()`.
    """
    p = ArgumentParser()
    p.add_argument(
        "-w",
        dest="worker_count_per_proc",
        type=int,
        help="Number of worker threads per process",
        default=1,
    )
    p.add_argument(
        "-n",
        dest="proc_count",
        type=int,
        help="Number of processes to start",
        default=1,
    )
    out = p.parse_args(args)

    kwargs = {
        "proc_count": out.proc_count,
        "worker_count_per_proc": out.worker_count_per_proc,
    }
    return kwargs


def proc_env(env: Dict[str, str] = os.environ) -> Dict[str, Any]:
    """Parse environment variables to generate arguments for
    `cluster_main()` when you are manually launching a cluster.

    This is probably what you want to use in Kubernetes.

    See documentation for `cluster_main()` for semantics of these
    variables.

    The environment variables you need set are:

    * `BYTEWAX_WORKERS_PER_PROCESS`

    Then either:

    * `BYTEWAX_ADDRESSES` - `;` separated list of "host:port"
      addresses.

    * `BYTEWAX_HOSTFILE_PATH` - Path to a file containing a list of
      cluster addresses.

    Then either:

    * `BYTEWAX_PROCESS_ID`

    * `BYTEWAX_POD_NAME` and `BYTEWAX_STATEFULSET_NAME` -
      E.g. `cluster_name-0` and `cluster_name` and we will calculate
      the process ID from that.

    >>> __skip_doctest_on_win_gha()
    >>> from bytewax import Dataflow, cluster_main
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> ib = lambda i, n: enumerate(range(3))
    >>> ob = lambda i, n: print
    >>> env = {
    ...     "BYTEWAX_ADDRESSES": "localhost:2101",
    ...     "BYTEWAX_PROCESS_ID": "0",
    ...     "BYTEWAX_WORKERS_PER_PROCESS": "2",
    ... }
    >>> cluster_main(flow, ib, ob, **proc_env(env))  # doctest: +ELLIPSIS
    (0, 0)
    ...
    (2, 2)

    Args:
        env: Environment variables. Defaults to `os.environ`.
    Returns: kwargs to pass to `cluster_main()`.
    """
    if "BYTEWAX_ADDRESSES" in env:
        addresses = env["BYTEWAX_ADDRESSES"].split(";")
    else:
        with open(env["BYTEWAX_HOSTFILE_PATH"]) as hostfile:
            addresses = [
                address.strip() for address in hostfile if address.strip() != ""
            ]

    if "BYTEWAX_PROCESS_ID" in env:
        proc_id = int(env["BYTEWAX_PROCESS_ID"])
    else:
        proc_id = int(
            env["BYTEWAX_POD_NAME"].replace(env["BYTEWAX_STATEFULSET_NAME"] + "-", "")
        )

    kwargs = {
        "worker_count_per_proc": int(env["BYTEWAX_WORKERS_PER_PROCESS"]),
        "addresses": addresses,
        "proc_id": proc_id,
    }
    return kwargs


def proc_args(args: Iterable[str] = None) -> Dict[str, Any]:
    """Parse command line arguments to generate arguments for
    `cluster_main()` when you are manually launching a cluster.

    See documentation for `cluster_main()` for semantics of these
    variables.

    >>> __skip_doctest_on_win_gha()
    >>> from bytewax import Dataflow, cluster_main
    >>> flow = Dataflow()
    >>> flow.capture()
    >>> ib = lambda i, n: enumerate(range(3))
    >>> ob = lambda i, n: print
    >>> args = "-w2 -p0 -a localhost:2101".split()
    >>> cluster_main(flow, ib, ob, **proc_args(args))  # doctest: +ELLIPSIS
    (0, 0)
    ...
    (2, 2)

    Args:
        args: List of arguments to parse. Defaults to `sys.argv`.
    Returns: kwargs to pass to `cluster_main()`.
    """
    p = ArgumentParser()
    p.add_argument(
        "-w",
        dest="worker_count_per_proc",
        type=int,
        help="Number of worker threads per process",
        default=1,
    )
    p.add_argument(
        "-p",
        dest="proc_id",
        type=int,
        required=True,
        help="Index of this process in cluster, starts from 0",
    )
    p.add_argument(
        "-a",
        dest="addresses",
        action="append",
        required=True,
        help="Add the hostname:port address of every (including this) process in cluster",
    )
    out = p.parse_args(args)

    kwargs = {
        "worker_count_per_proc": out.worker_count_per_proc,
        "addresses": out.addresses,
        "proc_id": out.proc_id,
    }
    return kwargs
