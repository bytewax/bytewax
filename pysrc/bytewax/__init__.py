"""Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

[See our readme for more
documentation.](https://github.com/bytewax/bytewax)

# Getting Started

A Bytewax **dataflow** is a fixed directed acyclic graph of
computational **steps** which are preformed on a possibly-unbounded
stream of data. Each step is made up of an **operator** or a specific
shape of computation (e.g. "transform each item individually" /
`bytewax.operators.map.map`). You define a dataflow via Python code
and then run the dataflow and it goes out and f

## Baby Steps

Create a `bytewax.dataflow.Dataflow` instance in a variable named
`flow`, then use the operator methods loaded onit to add
computational steps as a "fluent" API. Each operator method will
return a new stream with the results which you can attach more
operators to.

The dataflow and each operator method take a unique **step ID** you
should set to a name that represents the purpose of this computational
step.

>>> from bytewax.dataflow import Dataflow
>>> from bytewax.testing import TestingSource
>>> from bytewax.connectors.stdio import StdOutSink
>>> flow = Dataflow("my_flow")
>>> nums = flow.input("nums", TestingSource([1, 2, 3]))
>>> nums = nums.map("double", lambda x: x * 2)
>>> nums.output("print", StdOutSink())
2
4
6

To run your dataflow, execute the `bytewax.run` module with the Python
import path to the file you just saved your `Dataflow` in:

```
$ python -m bytewax.run examples.simple
```

## Dataflow Graphs

Each stream can be referenced as many times as you want to process a
copy of all of the data in the stream. Notice below the `nums` stream
is duplicated so it can be both doubled and multipled by ten.

>>> flow = Dataflow("my_flow")
>>> nums = flow.input("nums", TestingSource([1, 2, 3]))
>>> doubles = nums.map("double", lambda x: x * 2)
>>> tens = nums.map("tens", lambda x: x * 10)
>>> all = doubles.merge("merge", tens)
>>> all.output("print", StdOutSink())
2
10
4
20
6
30

## Stateful Operators



# Diving Deeper

## Getting Comfortable

The operators in `bytewax.operators` for all the built-in operators
and some simple examples of how to use each operator.

See the `bytewax.connectors` module for all the built-in input and
output connectors.

See the `bytewax.run` module docstring to learn about more ways to
execute dataflows.

## Advanced Material

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
