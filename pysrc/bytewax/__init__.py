"""Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

Bytewax is a Python-native binding to the [Timely
Dataflow](https://github.com/TimelyDataflow/timely-dataflow) library.
Timely Dataflow is a distributed dataflow runtime written in
[Rust](https://www.rust-lang.org/).

# Benefits

At a high level, Bytewax provides a few major benefits:

- You can develop and execute your code locally, and then easily
  horizontally scale that code to a cluster or Kubernetes without
  changes.

- A self-hosted Kubernetes platform product that helps out with
  operational tasks like logging and metrics. And the
  [`waxctl`](https://bytewax.io/docs/deployment/waxctl) to help manage

- The Bytewax runtime provides a recovery system which automatically
  backs up the state in your dataflow and allows you to recover from
  failure without slowly re-processing all data.

- A built-in library of connectors to easily allow integration with
  external systems, and the ability to write your own connectors in
  pure Python.

- A built-in library of operators to handle common data transformation
  use cases, and the ability to write your own in pure Python.

# Installation

Bytewax currently supports the following versions of Python: `3.8`,
`3.9`, `3.10`, and `3.11`.

We recommend creating a virtual environment when installing Python
dependencies. For more information on setting up a virtual
environment, see the [virtual environment Python
documentation](https://docs.python.org/3/tutorial/venv.html).

```bash
pip install bytewax
```

# Getting Started

Dataflow programming is a programming paradigm where program execution
is conceptualized as data flowing through a series of operations.

A Bytewax **dataflow** is a fixed directed acyclic graph of
computational **steps** which are preformed on a possibly-unbounded
stream of data. Each step is made up of an **operator** or a specific
shape of computation (e.g. "transform each item individually" /
`bytewax.operators.map`). You define a dataflow via Python code and
run the dataflow, and it goes out and polls the input for new items
and automatically pushes them through the steps until they reach an
output. An **item** is a single Python object that is flowing through
the dataflow.

A dataflow is defined in a Python file by creating a variable named
`flow` that is a `bytewax.dataflow.Dataflow`. You then reference this
file to run the dataflow. Let's make a very simple example dataflow
now.

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

## Operators

**Operators** are the processing primitives of Bytewax. Each of them
gives you a "shape" of data transformation, and you give them
functions to customize them to a specific task you need. The
combination of each operator and their custom logic functions we call
a dataflow **step**. You chain together steps in a dataflow to solve
your high-level data processing problem.

If you've ever used Python's
[`map`](https://docs.python.org/3/library/functions.html#map) or
[`filter`](https://docs.python.org/3/library/functions.html#filter) or
`functool`'s
[`reduce`](https://docs.python.org/3/library/functools.html#functools.reduce)
or equivalent in other languages, operators are the same concept. If
not, no worries, there's an example provided in the documentation for
each operator in `bytewax.operators`.

Each operator method takes a **step name** you should set to a
unique name that represents the purpose of this computational step.
This gets attached to errors, logging, and metrics so you can link
these bits of information for debugging.

Here's an example that uses a few more operators.

TODO Word count again?

## Streams

Each operator takes and returns one or more
`bytewax.dataflow.Stream`s. Each **stream** is a handle to that
specific flow of items. The streams that are arguments to operators we
call **upstreams** and returns some streams we call **downstreams**.

Each stream can be referenced as many times as you want to process a
copy of the data in the stream in a different way. Notice below the
`nums` stream is referenced twice below so it can be both doubled and
multipled by ten. `bytewax.operators.merge` is an operator that does
the reverse, and combines together multiple streams.

>>> flow = Dataflow("branching_math")
>>> nums = op.input("nums", flow, TestingSource([1, 2, 3]))
>>> doubles = op.map("do_double", nums, lambda x: x * 2)
>>> tens = op.map("do_tens", nums, lambda x: x * 10)
>>> all = op.merge("merge", doubles, tens)
>>> op.output("print", all, StdOutSink())
>>> bytewax.testing.run_main(flow)
2
10
4
20
6
30

## Debugging

### Inspecting

Bytewax provides the `bytewax.operators.inspect` operator to help you
visualize what is going on inside your dataflow. It prints out the
repr of all items that pass through it when you run the dataflow. You
can attach it to any stream you'd like to see.

TODO Example?

### Step Names and Step IDs

Operator methods take a string step name as their first argument which
should represent the semantic purpose of that computational step.

Bytewax operators are defined in terms of other operators
hierarchically in nested **substeps**. In order to disambiguate
information about substeps under a single top-level step, each substep
is assigned a unique **step ID**, which is a dotted path of all the
step names down the heirarchy to that individual operator.

This means exceptions, logs, traces, and metrics might show step IDs
like `branching_math.do_double.flat_map`. Don't fret! Read them from
left to right: the first component will be the flow ID, then the
top-level step name, then the path to the specific inner substep the
information applies to.

Just because an exception references a step ID of a substep of one of
your steps, does not mean the bug is in the inner step. It's possible
some logic function or value supplied is not conforming to the
contract the operator set. You'll need to read the exception and
documentation of the operator carefully.

## Stateful Operators and Keyed Streams

Bytewax has **stateful operators** that allow you to persist state
between items flowing in the dataflow. Some examples are
`bytewax.operators.window.fold_window` and
`bytewax.operators.stateful_map`. The Bytewax runtime partitions state
across multiple workers by a **state key**. This means that for a
given key, all items are moved onto the primary worker which has the
state for the key before processing the step. This means the state
overall is balanced over all workers, allowing rescaling, but the
state for a single key is localized.

This means that all stateful operators only work with a **keyed
stream** upstream with items that are shaped like 2-tuples of `(key,
value)`. Whenever we refer to a **value** in reference to stateful
operators, it means specifically the second value of these tuples. The
operators `bytewax.operators.key_on` and
`bytewax.operators.key_assert` help you make streams of this
`bytewax.dataflow.KeyedStream` shape. Downstreams of the stateful
operators will also be keyed and the relevant key will automatically
be applied.

For example, let's write a small dataflow that calculates a running
sum of points in a basketball game. First, let's define a

>>> flow = Dataflow("running_sum")
>>> points_a = op.input("points_a", flow, TestingSource([2, 2, 3, 2, 3, 3]))
>>> points_b = op.input("points_b", flow, TestingSource([2, 3, 2, 2, 2, 2]))
>>> points_a = op.key_on("key_a", points_a, lambda _x: "A")
>>> points_b = op.key_on("key_b", points_b, lambda _x: "B")
>>> points = op.merge("merge", points_a, points_b)
>>> def running_sum(old_sum, just_scored_points):
...     new_sum = old_sum + just_scored_points
...     return (new_sum, new_sum)
>>> running_sum = op.stateful_map("running_sum", points, lambda: 0, running_sum)
>>> op.output("out", running_sum, StdOutSink())
>>> bytewax.testing.run_main(flow)
('A', 2)
('B', 2)
('A', 4)
('B', 5)
('A', 7)
('B', 7)
('A', 9)
('B', 9)
('A', 12)
('B', 11)
('A', 15)
('B', 13)

## Input and Output

TODO Explain the basics of **sources**, **sinks**. Link to
`bytewax.inputs` and `bytewax.outputs` for more info.

We have a buffet of built-in

# Diving Deeper

## Getting Comfortable

See the `bytewax.operators` module for all the built-in operators and
some simple examples of how to use each operator.

See the `bytewax.connectors` module for all the built-in input and
output connectors.

See the `bytewax.run` module docstring to learn about more ways to
execute dataflows.

Consult the [Bytewax
glossary](https://bytewax.io/docs/reference/glossary) if you run
across a term you don't know.

## Production Deployments

See the documentation on
[`waxctl`](https://bytewax.io/docs/deployment/waxctl), our Kubernetes
tool for managing deployed dataflows.

See the `bytewax.recovery` module docstring on setting up failure
recovery.

See the `bytewax.tracing` module docstring for how to setup metrics
and tracing.

## Advanced Material

See the `bytewax.inputs` and `bytewax.outputs` module docstrings to
learn about how to write your own custom input and output connectors.

See the `bytewax.dataflow` module docstring to learn how to write your
own operators.

"""  # noqa: D205

__pdoc__ = {
    # This is the PyO3 module that has to be named "bytewax". Hide it
    # since we re-import its members into the Python source files in
    # their final locations.
    "bytewax": False,
}
