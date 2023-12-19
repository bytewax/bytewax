Let's look at an end-to-end example using Bytewax. We'll start by building out a
simple dataflow that performs a count of words in a file.

To begin, save a copy of this text in a file called `wordcount.txt`:

```
To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.
```

And a copy of the code in a file called `wordcount.py`.

```python
import operator
import re

from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink


flow = Dataflow("wordcount_eg")
inp = op.input("inp", flow, FileSource("wordcount.txt"))


def lower(line):
    return line.lower()


lowers = op.map("lowercase_words", inp, lower)


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


tokens = op.flat_map("tokenize_input", lowers, tokenize)

counts = op.count_final("count", tokens, lambda word: word)

op.output("out", counts, StdOutSink())
```

## Running the example

Now that we have our program and our input, we can run our example to see it in action:

```shell
> python -m bytewax.run wordcount
("'tis", 1)
('a', 1)
('against', 1)
('and', 2)
('arms', 1)
('arrows', 1)
('be', 2)
('by', 1)
('end', 1)
('fortune', 1)
('in', 1)
('is', 1)
('mind', 1)
('nobler', 1)
('not', 1)
('of', 2)
('opposing', 1)
('or', 2)
('outrageous', 1)
('question', 1)
('sea', 1)
('slings', 1)
('suffer', 1)
('take', 1)
('that', 1)
('the', 3)
('them', 1)
('to', 4)
('troubles', 1)
('whether', 1)
```

## Unpacking the program

Now that we've run our dataflow, let's walk through the components that we used.

In a dataflow program, each step added to the flow will occur in the
order that it is added. For our wordcount dataflow, we'll want the
following steps:

- Take a line from the file
- Lowercase all characters in the line
- Split the line into words
- Count the occurrence of each word in the file
- Print out the result after all the lines have been processed

We'll start with how to get input we'll push through our dataflow.

### Take a line from the file

Let's define the steps that we want to execute for each line of input
that we receive. We will add these steps as chains of **operators** on
a **dataflow object**, `bytewax.dataflow.Dataflow`.

```python
flow = Dataflow("wordcount_eg")
inp = op.input("inp", flow, FileSource("wordcount.txt"))
```

To emit input into our dataflow, our program needs an **input
operator** that takes an **source**. To start, we'll use one of our
prepackaged sources, `bytewax.connectors.files.FileSource`. This will
read the text file line-by-line and emit each line into the dataflow
at that point.

To read more about other options for sources, see the [module docs for
`bytewax.connectors`](/apidocs/bytewax.connectors/index) for information on
how to make your own custom sources, see [the module docs for `bytewax.inputs`](/apidocs/bytewax.inputs).

### Lowercase all characters in the line

If you look closely at our input, we have instances of both `To` and `to`. Let's add a step to our dataflow that transforms each line into lowercase letters. At the same time, we'll introduce the [map](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.map) operator.

```python
def lower(line):
    return line.lower()


lowers = op.map("lowercase_words", inp, lower)
```

For each item that our generator produces, the map operator will use the [built-in string function `lower()`](https://docs.python.org/3/library/stdtypes.html#str.lower) to emit downstream a copy of the string with all characters converted to lowercase.

### Split the line into words

When our `input_builder()` function is called, it will receive an entire line from our file. In order to count the words in the file, we'll need to break that line up into individual words.

Enter our `tokenize()` function, which uses a Python regular expression to split the line of input into a list of words:

```python
def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)
```

For example,

```python
to_be = "To be, or not to be, that is the question:"
print(tokenize(to_be))
```

results in:

```{testoutput}
['To', 'be', 'or', 'not', 'to', 'be', 'that', 'is', 'the', 'question']
```

To make use of `tokenize` function, we'll use the [flat map operator](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.flat_map):

```python
tokens = op.flat_map("tokenize_input", lowers, tokenize)
```

The flat map operator defines a step which calls a function on each input item. Each word in the list we return from our function will then be emitted downstream individually.

### Build up counts

At this point in the dataflow, the items of data are the individual words. In order to tally counts of words, we'll need to be able
to group words together.

We can use the [`count_final` operator](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.count_final) to
produce a count of all items in a dataflow. The `count_final` operator should only be used in a dataflow
that is run in a batch context, as it waits for all data to be read before producing output. In this
example, we want to count all of the items in the entire file before returning the result.

`count_final` takes a function that produces a key for each item in the dataflow. Many operators
in Bytewax require their input stream to be keyed, to ensure that all items for a given key
are processed together. In our word count example, we can use the word itself as the key,
so that each instance of that word is counted together.

```python
counts = op.count_final("count", tokens, lambda word: word)
```

### Print out the counts

The last part of our dataflow program will use an [output operator](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.output) to mark the output of our reduction as the dataflow's final output.

```python
op.output("out", counts, StdOutSink())
```

This means that whatever items are flowing through this point in the
dataflow will be passed on to a **sink**. We use
[StdOutSink](/apidocs/bytewax.connectors/stdio#bytewax.connectors.stdio.StdOutSink)
to route our output to the system's standard output.

### Running

To run the example, we just need to call the execution script.
We will talk in more details about it in the next chapter.
When we call `bytewax.run`, our dataflow program will begin running, Bytewax will read the input items from our input generator, push the data through each step in the dataflow, and return the captured output. We then print the output of the final step.

Here is the complete output when running the example:

```shell
> python -m bytewax.run wordcount
('opposing', 1)
('and', 2)
('of', 2)
('end', 1)
('whether', 1)
('arrows', 1)
('that', 1)
('them', 1)
('not', 1)
('by', 1)
('sea', 1)
('arms', 1)
('a', 1)
('is', 1)
('against', 1)
('to', 4)
("'tis", 1)
('nobler', 1)
('take', 1)
('question', 1)
('troubles', 1)
('or', 2)
('slings', 1)
('mind', 1)
('outrageous', 1)
('suffer', 1)
('be', 2)
('in', 1)
('the', 3)
('fortune', 1)
```

To learn more about possible modes of execution, [read our page on execution](docs/getting-started/execution.md)
