Now that we've installed bytewax, let's begin with an end-to-end
example. We'll start by building out a simple dataflow that performs
count of words in a file.

To begin, save a copy of this text in a file called `wordcount.txt`:

```
To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.
```

And a copy of the code in a file called `wordcount.py`.

```python doctest:SORT_OUTPUT doctest:ELLIPSIS doctest:NORMALIZE_WHITESPACE
import operator
import re

from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileInput
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, TumblingWindow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


clock_config = SystemClockConfig()
window_config = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

flow = Dataflow()
flow.input("inp", FileInput("wordcount.txt"))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("sum", clock_config, window_config, add)
flow.output("out", StdOutput())
```

## Running the example

Now that we have our program and our input, we can run our example via
`python -m bytewax.run wordcount:flow` and see the completed result:


```
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

Now that we've run our first bytewax program, let's walk through the
components that we used.

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
flow = Dataflow()
flow.input("input", FileInput("wordcount.txt"))
```

To emit input into our dataflow, our program needs an **input
operator** that takes an **input object**. To start, we'll use one of
our prepackaged inputs, `bytewax.connectors.files.FileInput`. This
will read the text file line-by-line and emit each line into the
dataflow at that point.

To read more about other options for input, see the [module docs for
`bytewax.connectors`](/apidocs/bytewax.connectors/index) or on how to
make your own custom inputs, see [the module docs for
`bytewax.inputs`](/apidocs/bytewax.inputs).

### Lowercase all characters in the line

If you look closely at our input, we have instances of both `To` and `to`. Let's add a step to our dataflow that transforms each line into lowercase letters. At the same time, we'll introduce the [map](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.map) operator.

```python
def lower(line):
    return line.lower()


flow.map(lower)
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
flow.flat_map(tokenize)
```

The flat map operator defines a step which calls a function on each input item. Each word in the list we return from our function will then be emitted downstream individually.

### Build up counts

At this point in the dataflow, the items of data are the individual words.

Let's skip ahead to the second operator here, [reduce window](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.reduce_window).

```python
def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


# Configuration for time based windows.
clock_config = SystemClockConfig()
window_config = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

flow.map(initial_count)
flow.reduce_window("sum", clock_config, window_config, add)
```

Its super power is that it can repeatedly combine together items into a single, aggregate value via a reducing function. Think about it like reducing a sauce while cooking: you are boiling all of the values down to something more concentrated.

In this case, we pass it the reducing function `add()` which will sum together the counts of words so that the final aggregator value is the total.

How does `reduce_window` know which items to combine? Part of its requirements are that the input items from the previous step in the dataflow are `(key, value)` two-tuples, and it will make sure that all values for a given key are passed to the reducing function. Thus, if we make the word the key, we'll be able to get separate counts!

That explains the previous map step in the dataflow with `initial_count()`.

This map sets up the shape that `reduce_window` needs: two-tuples where the key is the word, and the value is something we can add together. In this case, since we have a copy of a word for each instance, it represents that we should add `1` to the total count, so label that here.

How does reduce_window know **when** to emit combined items? That is what `clock_config` and `window_config` are for.
[SystemClockConfig](/apidocs/bytewax.window#bytewax.window.SystemClockConfig) is used to synchronize the flow's clock to the system one.
[TumblingWindow](/apidocs/bytewax.window#bytewax.window.TumblingWindow) instructs the flow to close windows every `length` period, 5 seconds in our case.
`reduce_window` will emit the accumulated value every 5 seconds, and once the input is completely consumed.


### Print out the counts

The last part of our dataflow program will use an [output operator](/apidocs/bytewax.dataflow#bytewax.dataflow.Dataflow.output) to mark the output of our reduction as the dataflow's final output.

```python
flow.output("out", StdOutput())
```

This means that whatever items are flowing through this point in the dataflow will be passed on as output. We use [StdOutput](/apidocs/bytewax.connectors.stdio#bytewax.connectors.stdio.StdOutput) to route our output to the system's standard output.

### Running

To run the example, we just need to call the execution script.
We will talk in more details about it in the next chapter.
When we call `bytewax.run`, our dataflow program will begin running, Bytewax will read the input items from our input generator, push the data through each step in the dataflow, and return the captured output. We then print the output of the final step.

Here is the complete output when running the example:

```
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

To learn more about possible modes of execution, [read our page on execution](/docs/getting-started/execution/).
