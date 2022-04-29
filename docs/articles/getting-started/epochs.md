Now that we've seen an end-to-end example running a dataflow program, it's time to discuss epochs.

To explain what an epoch represents, let's imagine an unending stream of lines of text that we want to count words for. If the stream has no end, we'll need to introduce another concept to decide when we want to emit the current counts of words from the stream.

**Epochs** are what allow us to influence ordering and batching within a dataflow. Increment the epoch when you know you need some data to be processed strictly after, or separately from the proceeding data.

## Input example

In our wordcount example in the previous section, we defined a `file_input()` generator to be our input iterator:

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
import re

from bytewax import Dataflow, run


def file_input():
    for line in open("wordcount.txt"):
        yield 1, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1
    
    
def add(count1, count2):
    return count1 + count2


flow = Dataflow()
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_epoch(add)
flow.capture()


for epoch, item in run(flow, file_input()):
    print(item)
```

For each line in our file, the input builder returned a two-tuple of `1`, and the line from the file.

The `1` in this example is what we refer to as an epoch. In our initial wordcount example, we're always emitting `1` as our epoch. This has the effect of counting all the words in our file together, as they all share the same epoch:

```{testoutput}
('fortune', 1)
('a', 1)
('whether', 1)
('them', 1)
('nobler', 1)
("'tis", 1)
('arrows', 1)
('slings', 1)
('and', 2)
('outrageous', 1)
('sea', 1)
('by', 1)
('or', 2)
('not', 1)
('opposing', 1)
('question', 1)
('mind', 1)
('that', 1)
('to', 4)
('end', 1)
('arms', 1)
('suffer', 1)
('take', 1)
('in', 1)
('against', 1)
('is', 1)
('of', 2)
('the', 3)
('be', 2)
('troubles', 1)
```

## Multiple epochs

What if we modify our input to increment the epoch that is emitted for each line in the file?


```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def file_input():
    for i, line in enumerate(open("wordcount.txt")):
        yield i, line
        
        
for epoch, item in run(flow, file_input()):
    print(item)
```

If we modify our function in this way, our final counts will change to count the words in each _line_ in the file, instead of the counts of words in the entire file.

Our output will now look like this:

```{testoutput}
('not', 1)
('that', 1)
('or', 1)
('the', 1)
('be', 2)
('is', 1)
('question', 1)
('to', 2)
('mind', 1)
("'tis", 1)
('to', 1)
('nobler', 1)
('the', 1)
('in', 1)
('whether', 1)
('suffer', 1)
('the', 1)
('of', 1)
('slings', 1)
('arrows', 1)
('outrageous', 1)
('fortune', 1)
('and', 1)
('troubles', 1)
('sea', 1)
('against', 1)
('to', 1)
('of', 1)
('a', 1)
('or', 1)
('take', 1)
('arms', 1)
('them', 1)
('opposing', 1)
('end', 1)
('and', 1)
('by', 1)
```

Notice how there are multiple instances of counts of `to` and `and`, etc!

## Inspecting epochs

To better illustrate that counts are per-line, we can print the epoch in addition to the output items when we run the dataflow:

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
for epoch, item in run(flow, file_input()):
    print(epoch, item)
```

Running the program again:

```{testoutput}
0 ('to', 2)
0 ('question', 1)
0 ('is', 1)
0 ('not', 1)
0 ('that', 1)
0 ('or', 1)
0 ('be', 2)
0 ('the', 1)
1 ("'tis", 1)
1 ('in', 1)
1 ('to', 1)
1 ('suffer', 1)
1 ('whether', 1)
2 ('slings', 1)
2 ('of', 1)
2 ('arrows', 1)
1 ('nobler', 1)
1 ('the', 1)
1 ('mind', 1)
2 ('the', 1)
2 ('and', 1)
2 ('fortune', 1)
2 ('outrageous', 1)
3 ('of', 1)
3 ('arms', 1)
3 ('against', 1)
3 ('to', 1)
3 ('take', 1)
3 ('a', 1)
3 ('sea', 1)
3 ('or', 1)
3 ('troubles', 1)
4 ('and', 1)
4 ('end', 1)
4 ('them', 1)
4 ('by', 1)
4 ('opposing', 1)
```

Since each line in the file corresponds to an epoch, we get counts grouped by line.

## Epoch-aware operators and ordering

This different grouping happened "automatically" because of the behavior of the [reduce epoch](/apidocs#bytewax.Dataflow.reduce_epoch) operator. It "ends" its window of aggregation for each key at the end of each epoch.

There are a few other operators that use the epoch as part of their semantcs: [reduce](/apidocs#bytewax.Dataflow.reduce), [stateful_map](/apidocs#bytewax.Dataflow.stateful_map), and others. You'll have to read the specifications of each to know how it interacts with epochs and what kind of ordering guarantees it gives you.

On that note, remember unless explicitly declared by a given operator _there are no ordering guarantees_. Bytewax might run your operator functions with data in any epoch order, and there's usually no order within an epoch!

For example, especially in a multi-worker and multi-process execution, this behavior could happen:

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
flow = Dataflow()
flow.capture()

for epoch, item in run(flow, enumerate(range(6))):
    print(epoch, item)
```

```{testoutput}
5 5
2 2
0 0
1 1
3 3
4 4
```

If you actually care about order, you _must_ use epochs to describe that requirement to bytewax.

## Input helpers

There are some **input helpers** in the `bytewax.inputs` module which allow you to wrap iterators that don't include an epoch with some standard behaviors for how you might want to assign timestamps:

- **Single batch** assigns all input to a constant epoch.
- **Tumbling epoch** increments the epoch every number of seconds.
- **Fully ordered** increments the epoch every item.

These allow you to conveniently assign epochs, but you can also assign them by having the input generator return two-tuples of `(epoch, item)` like in the wordcount example.

## A closer look at epochs

As mentioned previously, bytewax will "automatically" produce results from operators like [reduce epoch](/apidocs#bytewax.Dataflow.reduce_epoch) by "ending" its window of aggregation for each key at the end of each epoch. Let's look more closely at how this is accomplished, and how we can use this to control when work is done with bytewax.

To do that, let's examine how `run()` works under the hood.

Inside of the run function, the generator of input (named `inp` here) yields tuples of (epoch, input). That input generator is wrapped in an input_builder function:

``` python
def input_builder(worker_index, worker_count):
    assert worker_index == 0
    for epoch, input in inp:
        yield AdvanceTo(epoch)
        yield Emit(input)
```

Here we introduce two new primitives in Bytewax: [`AdvanceTo`](/apidocs#bytewax.AdvanceTo) and [`Emit`](/apidocs#bytewax.Emit).

These two dataclasses are how we inform Bytewax of new input and of the progress of epochs. `Emit` will introduce input into the dataflow at the current epoch, and `AdvanceTo` advances the current epoch to the value supplied.

Importantly, when you advance the epoch, you are informing bytewax that it will no longer see any input at any of the previous epoch values. At that point, bytewax will continue to do work until output is produced for the previous epoch.

