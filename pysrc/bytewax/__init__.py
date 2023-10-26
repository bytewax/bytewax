"""Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

[See our readme for more
documentation.](https://github.com/bytewax/bytewax)

# Getting Started

Create a `bytewax.dataflow.Dataflow` instance in a variable named
`flow`, then use the operator methods loaded onit to add computational
steps as a "fluent" API. Each operator method will return the
resulting streams which you can attach more operators to.

>>> from bytewax.dataflow import Dataflow
>>> from bytewax.testing import TestingSource
>>> from bytewax.connectors.stdio import StdOutSink
>>> flow = Dataflow("my_flow")
>>> nums = flow.input("nums", TestingSource([1, 2, 3]))
>>> nums.output("print", StdOutSink())
1
2
3

To run your dataflow, execute the `bytewax.run` module with the Python
import path to the file you just saved your `Dataflow` in:

```
$ python -m bytewax.run examples.simple
```

See the `bytewax.operators` module for all the built-in operators.

See the `bytewax.connectors` module for all the built-in input and
output connectors.

# Diving Deeper

The operators in `bytewax.operators` have some simple examples of how
to use each operator.

See the `bytewax.run` module docstring to learn about more ways to
execute dataflows.

See the `bytewax.inputs` and `bytewax.outputs` module docstrings to
learn about how to write your own custom input and output connectors.

See the `bytewax.dataflow` module docstring to learn how to write your
own operators.

"""  # noqa: D205

import bytewax.dataflow
import bytewax.operators

__pdoc__ = {
    # This is the PyO3 module that has to be named "bytewax". Hide it
    # since we re-import its members into the Python source files in
    # their final locations.
    "bytewax": False,
}


# Load all the built-in operators under their default names.
bytewax.dataflow.load_mod_ops(bytewax.operators)
