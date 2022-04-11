Now that we've seen an end-to-end example running a dataflow program, it's time to discuss **epochs**.

To explain what an epoch represents, let's imagine an unending stream of lines that we want to count words for. If the stream has no end, we'll need to introduce another concept to decide when we want to emit the current counts of words from the stream.

Epochs are what allow us to influence ordering and batching within a dataflow. Increment the epoch when you know you need some data to be processed strictly after, or separately from the proceeding data.

## Input example

In our wordcount example in the previous section, we defined a function for our input:

``` python
def file_input():
    for line in open("wordcount.txt"):
        yield 1, line
```

For each line in our file, we returned a two-tuple of `1`, and the line from the file.

The `1` in this example is what we refer to as an epoch. In our initial wordcount example, we're always emitting `1` as our epoch. This has the effect of counting all the words in our file together, as they all share the same epoch.

## Multiple epochs

What if we modify our input to increment the epoch that is emitted for each line in the file?


``` python
def file_input():
    for i, line in enumerate(open("wordcount.txt")):
        yield i, line
```

If we modify our function in this way, our final counts will change to count the words in each _line_ in the file, instead of the counts of words in the entire file.

Our output will now look like this:

```
('or', 1)
('not', 1)
('that', 1)
('is', 1)
('be', 2)
('the', 1)
('to', 2)
('question', 1)
('mind', 1)
('suffer', 1)
("'tis", 1)
('the', 1)
('to', 1)
('nobler', 1)
('whether', 1)
('in', 1)
('slings', 1)
('arrows', 1)
('outrageous', 1)
('fortune', 1)
('of', 1)
('and', 1)
('the', 1)
('or', 1)
('troubles', 1)
('them', 1)
('of', 1)
('a', 1)
('take', 1)
('to', 1)
('against', 1)
('sea', 1)
('arms', 1)
('by', 1)
('and', 1)
('end', 1)
('opposing', 1)
```

Notice how there are multiple instances of counts of `to` and `and`, etc!

## Inspecting epochs

To better illustrate that counts are per-line, we can use the [inspect epoch](/operators/operators#inspect-epoch) operator instead of [inspect](/operators/operators#inspect):

``` python
flow.inspect_epoch(print)
```

Running the program again:

```
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

This different grouping happened "automatically" because of the behavior of the [reduce epoch](/operators/operators#reduce-epoch) operator. It "ends" its window of aggregation for each key at the end of each epoch.

There are a few other operators that use the epoch as part of their semantcs: [reduce](/operators/operators#reduce), [stateful_map](/operators/operators#stateful-map), and others. You'll have to read the specifications of each to know how it interacts with epochs and what kind of ordering guarantees it gives you.

On that note, remember unless explicitly declared by a given operator _there are no ordering guarantees_. Bytewax might run your operator functions with data in any epoch order, and there's usually no order within an epoch!

For example, especially in a multi-worker and multi-process execution, this behavior could happen:

```python
flow = Dataflow()
flow.inspect_epoch(print)

run(flow, enumerate(range(6)))
```

```
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

These allow you to conveniently assign epochs, but you can also assign them by having the input generator return tw-tuples of `(epoch, item)` like in the wordcount example.
