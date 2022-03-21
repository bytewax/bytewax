"""Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

[See our readme for more
documentation.](https://github.com/bytewax/bytewax)

"""
from .bytewax import Dataflow
from .execution import run, run_cluster, spawn_cluster, cluster_main

__all__ = [
    "Dataflow",
    "run",
    "run_cluster",
    "spawn_cluster",
    "cluster_main",
]

__pdoc__ = {
    # This is the PyO3 module that has to be named "bytewax". Hide it
    # since we import all its members here.
    "bytewax": False,
    # Hide execution because we import all its members here.
    "execution": False,
}
