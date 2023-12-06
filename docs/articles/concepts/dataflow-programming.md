# Dataflow programming

Dataflow programming is a programming paradigm where program execution is conceptualized as data flowing through a series of operations or transformations.

Here we also mention a few concepts about streaming in general. No need to go super-deep here, but everytime you want to explain something in the following examples, think if we should rather have it here instead.

I'd expect some overview of types of streaming, producer/consumer/partitions/idempotency/snapshots/delivery semantics and reference to other parts of our docs.

Basically, lay down some basics and maybe show some python code. It would be super hard to write this before examples so it should always be thought of a "place to put additional stuff to help make the example section easier to follow"

## Overview

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

```python
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("branching_math")
nums = op.input("nums", flow, TestingSource([1, 2, 3]))
doubles = op.map("do_double", nums, lambda x: x * 2)
tens = op.map("do_tens", nums, lambda x: x * 10)
all = op.merge("merge", doubles, tens)
op.output("print", all, StdOutSink())
run_main(flow)
```

```{testoutput}
2
10
4
20
6
30
```

## Quick Logic Functions

Many of the operators take **logic functions** which help you
customize their behavior in a structured way. The most verbose way
would be to `def logic(...)` a function that does what you need to do,
but any callable value can be used as-is, though!

This means you can use the following existing callables to help you
make code more concise:

- [Built-in
  functions](https://docs.python.org/3/library/functions.html)

- [Constructors or
  `__init__`](https://docs.python.org/3/tutorial/classes.html#class-objects)

- [Methods](https://docs.python.org/3/glossary.html#term-method)

You can also use
[lambdas](https://docs.python.org/3/tutorial/controlflow.html#lambda-expressions)
to quickly define one-off anonymous functions for simple custom logic.

For example, all of the following dataflows are equivalent.

Using a defined function:

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("use_def")


def split_sentence(sentence):
    return sentence.split()


s = op.input("inp", flow, TestingSource(["hello world"]))
s = op.flat_map("split", s, split_sentence)
_ = op.inspect("out", s)
run_main(flow)
```

```{testoutput}
use_def.out: 'hello'
use_def.out: 'world'
```

Or a lambda:

```python
flow = Dataflow("use_lambda")
s = op.input("inp", flow, TestingSource(["hello world"]))
s = op.flat_map("split", s, lambda s: s.split())
_ = op.inspect("out", s)
run_main(flow)
```

```{testoutput}
use_lambda.out: 'hello'
use_lambda.out: 'world'
```

Or an unbound method:

```python
flow = Dataflow("use_method")
s = op.input("inp", flow, TestingSource(["hello world"]))
s = op.flat_map("split", s, str.split)
_ = op.inspect("out", s)
run_main(flow)
```

```{testoutput}
use_method.out: 'hello'
use_method.out: 'world'
```
