(getting-started)=
# Simple Example

Let's write our first Bytewax dataflow. Be sure that you've followed
the instructions for installing Bytewax in
<project:/articles/getting-started/installing.md>.

## Imports

Our dataflow starts with a few imports, so let's create those now.

```python
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
```

## Dataflow

To begin, create a {py:obj}`~bytewax.dataflow.Dataflow` instance in a
variable named `flow`. This defines the empty dataflow we'll add steps
to.

```python
flow = Dataflow("a_simple_example")
```

## Input

Every dataflow requires input. For this example, we'll use the
{py:obj}`~bytewax.operators.input` operator together with a
{py:obj}`~bytewax.testing.TestingSource` to produce a stream of
`int`s.


```python
stream = op.input("input", flow, TestingSource(range(10)))
```

The {py:obj}`~bytewax.operators.input` operator returns a
{py:obj}`~bytewax.dataflow.Stream` of values. If you are using an
editor with a [language server setup](#lsp) with type hints, you can
see that the return type is a `Stream[int]` containing integers.

## Operators

Each operator method will return a new
{py:obj}`~bytewax.dataflow.Stream` with the results of the step which
you can call more operators on. Let's use the
{py:obj}`~bytewax.operators.map` operator to double each number from
our input stream.

```python
def times_two(inp: int) -> int:
    return inp * 2


double = op.map("double", stream, times_two)
```

## Output

Finally, let's add an output step. At least one
{py:obj}`~bytewax.operators.input` step and one
{py:obj}`~bytewax.operators.output` step are required on every
dataflow. We'll have our output directed to standard out using the
{py:obj}`~bytewax.connectors.stdio.StdOutSink`.

```python
op.output("out", double, StdOutSink())
```

## Running

When writing Bytewax dataflows for production use, you should run your
dataflow using the {py:obj}`bytewax.run` module. Let's see an example
that does just that.

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

The first argument passed to the {py:obj}`bytewax.run` module is a
dataflow getter string. Because we saved our
{py:obj}`~bytewax.dataflow.Dataflow` definition in a variable named
`flow` we don't need to supply it when running our dataflow from the
command line.

Note that just executing the Python file will _not run it!_ You must
use the {py:obj}`bytewax.run` script and it's options so it can setup
the runtime correctly.
