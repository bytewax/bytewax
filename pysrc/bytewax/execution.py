"""How to execute your dataflows.

Run an instantiated `bytewax.dataflow.Dataflow` using one of the entry
point functions in this module.


Epoch Configs
-------------

Epochs define the granularity of recovery in a bytewax dataflow. By default, we
snapshot recovery every 10 seconds. You should only need to set this if you are
testing the recovery system or are doing deep exactly-once integration work. Changing
this does not change the semantics of any of the operators.


"""
from typing import Any, Iterable, List, Optional, Tuple

from multiprocess import get_context

from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig

from .bytewax import (  # noqa: F401
    cluster_main,
    EpochConfig,
    PeriodicEpochConfig,
    run_main,
    TestingEpochConfig,
)

# Due to our package structure, we need to define __all__
# in any submodule as pdoc will not find the documentation
# for functions imported here, but defined in another submodule.
# See https://pdoc3.github.io/pdoc/doc/pdoc/#what-objects-are-documented
# for more information.
__all__ = [
    "run_main",
    "cluster_main",
    "spawn_cluster",
    "EpochConfig",
    "PeriodicEpochConfig",
    "TestingEpochConfig",
]


def _gen_addresses(proc_count: int) -> Iterable[str]:
    return [f"localhost:{proc_id + 2101}" for proc_id in range(proc_count)]


def spawn_cluster(
    flow: Dataflow,
    *,
    epoch_config: Optional[EpochConfig] = None,
    recovery_config: Optional[RecoveryConfig] = None,
    proc_count: int = 1,
    worker_count_per_proc: int = 1,
    mp_ctx=get_context("spawn"),
) -> List[Tuple[int, Any]]:
    """Execute a dataflow as a cluster of processes on this machine.

    Blocks until execution is complete.

    Starts up cluster processes for you and handles connecting them
    together. You'd commonly use this for notebook analysis that needs
    parallelism and higher throughput, or simple stand-alone demo
    programs.

    >>> from bytewax.testing import doctest_ctx
    >>> from bytewax.dataflow import Dataflow
    >>> from bytewax.inputs import TestingInputConfig
    >>> from bytewax.outputs import StdOutputConfig
    >>> flow = Dataflow()
    >>> flow.input("inp", TestingInputConfig(range(3)))
    >>> flow.capture(StdOutputConfig())
    >>> spawn_cluster(
    ...     flow,
    ...     proc_count=2,
    ...     mp_ctx=doctest_ctx,  # Outside a doctest, you'd skip this.
    ... )  # doctest: +ELLIPSIS
    (...)

    See `bytewax.run_main()` for a way to test input and output
    builders without the complexity of starting a cluster.

    See `bytewax.cluster_main()` for starting one process in a cluster
    in a distributed situation.

    Args:

        flow: Dataflow to run.

        epoch_config: A custom epoch config. You probably don't need
            this. See `EpochConfig` for more info.

        recovery_config: State recovery config. See
            `bytewax.recovery`. If `None`, state will not be
            persisted.

        proc_count: Number of processes to start.

        worker_count_per_proc: Number of worker threads to start on
            each process.

        mp_ctx: `multiprocessing` context to use. Use this to
            configure starting up subprocesses via spawn or
            fork. Defaults to spawn.

    """
    addresses = _gen_addresses(proc_count)
    with mp_ctx.Pool(processes=proc_count) as pool:
        futures = [
            pool.apply_async(
                cluster_main,
                (flow,),
                {
                    "epoch_config": epoch_config,
                    "recovery_config": recovery_config,
                    "addresses": addresses,
                    "proc_id": proc_id,
                    "worker_count_per_proc": worker_count_per_proc,
                },
            )
            for proc_id in range(proc_count)
        ]
        pool.close()

        for future in futures:
            # Will re-raise exceptions from subprocesses.
            future.get()

        pool.join()
