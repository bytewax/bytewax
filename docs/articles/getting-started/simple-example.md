# A Simple Example

Let's write our first Bytewax Dataflow. Be sure that you've followed the instructions
for installing bytewax in [Installing bytewax](/docs/articles/getting-started/installation.md).

## Imports

Every good Python program starts with a few imports, so let's create those now.

``` python
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
```

## Dataflow

A Bytewax Dataflow can be understood as a program that takes items from one or more
**Sources**, performs computation on them using **Operators**, and writes it's output to
one or more **Sinks** as output.

To begin, we'll create our dataflow, and name it:

``` python
flow = Dataflow("a_simple_example")
```

Storing the dataflow in a variable named `flow` will make it easier to run when we
get to that step.

## Input

Every Dataflow requires some input. For this basic example, we'll be using the `TestingSource`.
TestingSources are useful for testing, and for local development, but should not be used
in Production.

To give our Dataflow input, we use the input **Operator** from our imports above.

``` python
inp = op.input("input", flow, TestingSource(range(10)))
```

The `input` operator takes a few parameters: a name, the `Dataflow` we constructed above, and
a `Source`. In our case, we're using the `TestingSource`, which takes a Python iterable to
use for input.

The `input` operator returns a `Stream` as it's return value, that we can use in subsequent
steps.

## Operators

Now that we have our input, let's perform some computation with it. To do that, we'll use
one of the most common **Operators**: `map`.

A `map` operation is a one-to-one transformation of input. It takes as arguments, a name,
the input to transform, and a function to do the transformation.

``` python
def times_two(inp: int) -> int:
    return inp * 2


double = op.map("double", inp, times_two)
```

The `map` operator returns a `Stream` of transformed values. If you are using mypy, you
can see that the return type is a new `Stream` of transformed integers.

A Bytewax Dataflow can have zero or more transformation steps, but for this example we'll
just have one and move on to output.

## Output

In this example, we'll be using `StdOutSink` as our output, which will write to STDOUT.

``` python
op.output("out", double, StdOutSink())
```

## The full program

Great, we have everything we need for our completed program, which for this example
should be saved in a file called `basic.py`:

``` python
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("a_simple_example")

inp = op.input("input", flow, TestingSource(range(10)))


def times_two(inp: int) -> int:
    return inp * 2


double = op.map("double", inp, times_two)

op.output("out", double, StdOutSink())
```

Now that we have all of the component parts, we can run our dataflow:

``` bash
python -m bytewax.run basic
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
The dataflow getter string uses the format <dataflow-module>:<dataflow-getter>.
By default, the `dataflow-getter` part of the string uses the variable `flow`,
and as a result, we don't need to supply it.
