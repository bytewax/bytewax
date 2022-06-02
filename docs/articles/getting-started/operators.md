**Operators** are the processing primitives of bytewax.
Each of them gives you a "shape" of data transformation, and you give them functions to customize them to a specific task you need.
The combination of each operator and their custom logic functions we call a dataflow **step**.
You chain together steps in a dataflow to solve your high-level data processing problem.

If you've ever used Python's [`map`](https://docs.python.org/3/library/functions.html#map) or [`filter`](https://docs.python.org/3/library/functions.html#filter) or `functool`'s [`reduce`](https://docs.python.org/3/library/functools.html#functools.reduce) or equivalent in other languages, operators are the same concept.
If not, no worries, we'll give a quick overview of each and links to our annotated examples which demonstrate the use of each in a relevant way.

## Using Operators

You can add steps to your dataflow by calling the method with the name of the operator you'd like to use on a [`bytewax.Dataflow`](/apidocs#bytewax.Dataflow) instance.

There is a detailed description of every operator, it's behavior, and a simple example in the API docs for [`bytewax.Dataflow`](/apidocs#bytewax.Dataflow).

## Epoch Modification

Operators generally do not modify the epoch of the data that is passing through them. If they do, that will be called out explicitly in their API documentation. For example, [`bytewax.Dataflow.reduce()`](/apidocs#bytewax.Dataflow.reduce) mentions that its output will be only in the epoch of the most recent value for each key.

## Stateful Operators

Any operator which carries state between processing items is a
**stateful operator**.

In order to coordinate this state in a multiple-worker execution, all
stateful operators require that their input are make up of **keys**
and **values** in a `(key, value)` two-tuple. Keys must be
strings. Bytewax can then route the value to the worker that has the
relevant state. Any output from these operators will also be `(key,
output_item)` two-tuples as well.

## Recoverable Operators

Stateful operators which persist state across epochs are also **recoverable operators**.

They all take a `step_id` argument so that their state is backed up correctly.
