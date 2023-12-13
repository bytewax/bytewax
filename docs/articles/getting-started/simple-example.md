Let's write our first Bytewax Dataflow. Be sure that you've followed the instructions
for installing bytewax in [Installing bytewax](/docs/articles/getting-started/installation.md).

## Imports

Our dataflow starts with a few imports, so let's create those now.

```python
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
```

## Dataflow

To begin, create a `bytewax.dataflow.Dataflow` instance in a variable named
`flow`. This defines the empty dataflow we'll add steps to.

```python
flow = Dataflow("a_simple_example")
```

## Input

Every Dataflow requires input. For this example, we'll use the `bytewax.operators.input`
operator to create an initial stream of items. We'll start with an input source for testing
that emits a list of static Python `int`s.


```python
stream = op.input("input", flow, TestingSource(range(10)))
```

## Operators

Each operator method will return a new stream with the results which you can
call more operators on. Let's use the `bytewax.operators.map` operator to double
each number; the `map` operator runs a function on each item and emits each
result downstream.

```python
def times_two(inp: int) -> int:
    return inp * 2


double = op.map("double", stream, times_two)
```

The `map` operator returns a `Stream` of transformed values. If you are using mypy, you
can see that the return type is a new `Stream` of transformed integers.

## Output

Finally, let's add an `bytewax.operators.output` step. At least one
`bytewax.operators.input` step and one `bytewax.operators.output` step
are required on every dataflow.

```python
op.output("out", double, StdOutSink())
```

## Running a Bytewax dataflow

When writing Bytewax Dataflows for production use, you should run your dataflow using the
`bytewax.run` module. Let's see an example that does just that.

To begin, save the following code in a file called `basic.py`.

```python
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("a_simple_example")

stream = op.input("input", flow, TestingSource(range(10)))


def times_two(inp: int) -> int:
    return inp * 2


double = op.map("double", stream, times_two)

op.output("out", double, StdOutSink())
```
To run the dataflow, use the following command:

```bash
> python -m bytewax.run basic
0
2
4
6
8
10
12
14
16
18
```

The first argument passed to the bytewax.run module is a dataflow getter string.

The dataflow getter string uses the format `<dataflow-module>:<dataflow-getter>`.
By default, the `dataflow-getter` part of the string defaults to using
the variable `flow`. Because we saved our `Dataflow` definition in a variable
named `flow` we don't need to supply it when running our dataflow from the
command line.

Note that just executing the Python file will _not run it!_ You must
use the `bytewax.run` script and it's options so it can setup the
runtime correctly.
