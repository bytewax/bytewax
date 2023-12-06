# Getting Started

## Baby Steps

First, create a file called `baby_steps.py`. We'll add our Python
Bytewax code to that.

Then create a `bytewax.dataflow.Dataflow` instance in a variable named
`flow`. This defines the empty dataflow we'll add steps to.

>>> from bytewax.dataflow import Dataflow
>>> flow = Dataflow("do_math")

Now let's import all of Bytewax's built-in operators for use.

>>> import bytewax.operators as op

Then we use the `bytewax.operators.input` operator to create an
initial stream of items. We'll start with an input source just for
testing that emits a list of static Python `int`s.

>>> from bytewax.testing import TestingSource
>>> nums = op.input("nums", flow, TestingSource([1, 2, 3]))

Each operator method will return a new stream with the results which
you can call more operators on. Let's use the `bytewax.operators.map`
operator to double each number; the `map` operator runs a function on
each item and emits each result downstream.

>>> nums = op.map("double", nums, lambda x: x * 2)

Finally, let's add an `bytewax.operators.output` step. At least one
`bytewax.operators.input` step and one `bytewax.operators.output` step
are required on every dataflow.

>>> from bytewax.connectors.stdio import StdOutSink
>>> op.output("print", nums, StdOutSink())

Putting this all together, the file would look like:

>>> import bytewax.operators as op
>>> from bytewax.connectors.stdio import StdOutSink
>>> from bytewax.dataflow import Dataflow
>>> from bytewax.testing import TestingSource
>>> flow = Dataflow("do_math")
>>> nums = op.input("nums", flow, TestingSource([1, 2, 3]))
>>> nums = op.map("double", nums, lambda x: x * 2)
>>> op.output("print", nums, StdOutSink())

To run your dataflow, from your shell execute the `bytewax.run` module
with the Python import path to the file you just saved your `Dataflow`
in. For the above example, since it's saved in `baby_steps.py` in the
current directory:

```
$ python -m bytewax.run baby_steps
2
4
6
```

Note that just executing the Python file will _not run it!_ You must
use the `bytewax.run` script and it's options so it can setup the
runtime correctly.

```
$ python baby_steps.py
# Nothing here...
```

In our documentation, we'll use our unit testing method to show the
output of the dataflow right below it's definition for convenience.

>>> import bytewax.testing
>>> bytewax.testing.run_main(flow)
2
4
6

Realize that just defining the dataflow does not actually run it, and
_you should not use this unit testing method in general for your own
code._

See the `bytewax.run` module docstring for more info on ways to
execute your dataflows.
