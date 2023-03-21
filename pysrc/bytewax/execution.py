"""How to execute your dataflows.

Run an instantiated `bytewax.dataflow.Dataflow` using one of the entry
point functions in this module.
"""
from .bytewax import cluster_main, run_main  # noqa: F401

# Due to our package structure, we need to define __all__
# in any submodule as pdoc will not find the documentation
# for functions imported here, but defined in another submodule.
# See https://pdoc3.github.io/pdoc/doc/pdoc/#what-objects-are-documented
# for more information.
__all__ = [
    "run_main",
    "cluster_main",
]
