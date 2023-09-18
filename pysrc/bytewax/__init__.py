"""Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

[See our readme for more
documentation.](https://github.com/bytewax/bytewax)

"""  # noqa: D205

import bytewax.dataflow
import bytewax.operators

__all__ = []


__pdoc__ = {
    # This is the PyO3 module that has to be named "bytewax". Hide it
    # since we re-import its members into the Python source files in
    # their final locations.
    "bytewax": False,
}


# Load all the built-in operators under their default names.
bytewax.dataflow.load_mod_ops(bytewax.operators)
