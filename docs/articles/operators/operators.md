In this section, we'll cover the Operators that are available in bytewax, and how to use them.

**Operators** are the processing primitives of bytewax.
Each of them gives you a "shape" of data transformation, and you give them functions to customize them to a specific task you need.
The combination of each operator and their custom logic functions we call a dataflow **step**.
You chain together steps in a dataflow to solve your high-level data processing problem.

You can add steps to your dataflow by calling the method with the name of the operator you'd like to use on the `Dataflow` object.

If you've ever used Python's [`map`](https://docs.python.org/3/library/functions.html#map) or [`filter`](https://docs.python.org/3/library/functions.html#filter) or `functool`'s [`reduce`](https://docs.python.org/3/library/functools.html#functools.reduce) or equivalent in other languages, operators are the same concept.
If not, no worries, we'll give a quick overview of each and links to our annotated examples which demonstrate the use of each in a relevant way.

# List of Operators

## Capture

```
Dataflow.capture()
```

**Capture** is how you specify the output of the dataflow. Capture is required on every dataflow.

Whenever an item flows by a capture operator, the [output handler](./execution#builders) of the worker is called with that item and epoch. For `run()` and `run_cluster()` output handlers are setup for you that return the output as the return value.

The return value is ignored; it emits items downstream unmodified. You can add multiple capture operators to a dataflow and the output from each will be passed to the output handler.

```python
from bytewax import Dataflow, run


flow = Dataflow()
flow.capture()


inp = enumerate(range(3))
print(run(flow, inp))
```

```{testoutput}
[(0, 0), (1, 1), (2, 2)]
```

## Inspect

```
Dataflow.inspect(inspector: Callable[[Any], None])
```

**Inspect** allows you to observe, but not modify, items.

It calls a function `inspector(item: Any) => None` on each item.

The return value is ignored; it emits items downstream unmodified.

```python
def log(item):
    print("Saw", item)


flow = Dataflow()
flow.inspect(log)
flow.capture()


inp = enumerate(range(3))
for epoch, item in run(flow, inp):
    pass
```

```{testoutput}
Saw 0
Saw 1
Saw 2
```

### Examples

Inspect is pervasive and is in almost every example as a debugging tool.

### Common Use Cases

- Debugging
- Logging

## Map

```
Dataflow.map(mapper: Callable[[Any], Any])
```

**Map** is a one-to-one transformation of items.

It calls a function `mapper(item: Any) => transformed_x: Any` on each item.

It emits each transformed item downstream.

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def add_one(item):
    return item + 1


flow = Dataflow()
flow.map(add_one)
flow.capture()


inp = enumerate(range(3))
for epoch, item in run(flow, inp):
    print(item)
```

```{testoutput}
2
1
3
```

### Examples

Map is pervasive and in almost every example, but here are our most simple examples.

- [Basic](https://github.com/bytewax/bytewax/blob/main/examples/basic.py)
- [Translator](https://github.com/bytewax/bytewax/blob/main/examples/translator.py)

### Common Use Cases

- Turning JSON into objects for processing
- Extracting a key from a compound object

## Filter

```
Dataflow.filter(predicate: Callable[[Any], bool])
```

**Filter** selectively keeps only some items.

It calls a function `predicate(item: Any) => should_emit: bool` on each item.

It emits the item downstream unmodified if the predicate returns `True`.

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def is_odd(item):
    return item % 2 != 0


flow = Dataflow()
flow.filter(is_odd)
flow.capture()


inp = enumerate(range(4))
for epoch, item in run(flow, inp):
    print(item)
```

```{testoutput}
3
1
```

### Examples

- [Search Session](https://github.com/bytewax/bytewax/blob/main/examples/search_session.py)
- [Word Count](https://github.com/bytewax/bytewax/blob/main/examples/wordcount.py)

### Common Use Cases

- Selecting relevant events
- Removing empty events
- Removing sentinels
- Removing stop words

## Flat Map

```
Dataflow.flat_map(mapper: Callable[[Any], Iterable[Any]])
```

**Flat map** is a one-to-many transformation of items.

It calls a function `mapper(item: Any) => emit: Iterable[Any]` on each item.

It emits each element in the downstream iterator individually.

```python doctest:SORT_EXPECTED doctest:SORT_OUTPUT
def split_into_words(sentence):
    return sentence.split()


flow = Dataflow()
flow.flat_map(split_into_words)
flow.capture()


inp = enumerate(["hello world"])
for epoch, item in run(flow, inp):
    print(item)
```

```{testoutput}
world
hello
```

### Examples

- [Word Count](https://github.com/bytewax/bytewax/blob/main/examples/wordcount.py)
- [Twitter Stream](https://github.com/bytewax/bytewax/blob/main/examples/twitter_stream.py)
- [Search Session](https://github.com/bytewax/bytewax/blob/main/examples/search_session.py)
- [PageRank](https://github.com/bytewax/bytewax/blob/main/examples/page_rank.py)

### Common Use Cases

- Tokenizing
- Flattening hierarchical objects
- Breaking up aggregations for further processing

## Inspect Epoch

```
Dataflow.inspect_epoch(inspector: Callable[[int, Any], None])
```

**Inspect epoch** allows you to observe, but not modify, items and their epochs.

It calls a function `inspector(epoch: int, item: Any) => None` on each item with its epoch.

The return value is ignored; it emits items downstream unmodified.

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def log(epoch, item):
    print(f"Saw {item} @ {epoch}")


flow = Dataflow()
flow.inspect_epoch(log)
flow.capture()


inp = enumerate(range(3))
for epoch, item in run(flow, inp):
    pass
```

```{testoutput}
Saw 0 @ 0
Saw 2 @ 2
Saw 1 @ 1
```

### Examples

Inspect epoch is pervasive and used in many examples as a debugging tool.

### Common Use Cases

- Debugging
- Logging

## Reduce

```
Dataflow.reduce(
    step_id: str,
    reducer: Callable[[Any, Any], Any],
    is_complete: Callable[[Any], bool],
)
```

**Reduce** lets you combine items for a key into an aggregator in epoch order.

Since this is a stateful operator, it requires the input items are `(key, value)` tuples so it can ensure that all relevant values are routed to the relevant aggregator.
Use [Map](#map) beforehand to extract out the key.

It calls two functions:

- A `reducer(aggregator: Any, value: Any) => updated_aggregator: Any` which combines two values.
The aggregator is initially the first value seen for a key.
Values will be passed in epoch order, but no order is defined within an epoch.

- An `is_complete(updated_aggregator: Any) => should_emit: bool` which returns true if the most recent `(key, aggregator)` should be emitted downstream and the aggregator for that key forgotten.
If there was only a single value for a key, it is passed in as the aggregator here.

It emits `(key, aggregator)` tuples downstream when you tell it to.

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def user_as_key(event):
    return event["user"], [event]


def extend_session(session, events):
    session.extend(events)
    return session


def session_complete(session):
    return any(event["type"] == "logout" for event in session)


flow = Dataflow()
flow.map(user_as_key)
flow.inspect_epoch(lambda epoch, item: print("Saw", item, "@", epoch))
flow.reduce("sessionizer", extend_session, session_complete)
flow.capture()


inp = [
    (0, {"user": "a", "type": "login"}),
    (1, {"user": "a", "type": "post"}),
    (1, {"user": "b", "type": "login"}),
    (2, {"user": "a", "type": "logout"}),
    (3, {"user": "b", "type": "logout"}),
]
for epoch, item in run(flow, inp):
    print(epoch, item)
```

```{testoutput}
Saw ('a', [{'user': 'a', 'type': 'login'}]) @ 0
Saw ('b', [{'user': 'b', 'type': 'login'}]) @ 1
Saw ('a', [{'user': 'a', 'type': 'post'}]) @ 1
Saw ('a', [{'user': 'a', 'type': 'logout'}]) @ 2
Saw ('b', [{'user': 'b', 'type': 'logout'}]) @ 3
2 ('a', [{'user': 'a', 'type': 'login'}, {'user': 'a', 'type': 'post'}, {'user': 'a', 'type': 'logout'}])
3 ('b', [{'user': 'b', 'type': 'login'}, {'user': 'b', 'type': 'logout'}])
```

### Examples

- [Search Session](https://github.com/bytewax/bytewax/blob/main/examples/search_session.py)

### Common Use Cases

- Sessionization
- Emitting a summary of data that spans timestamps

## Reduce Epoch

```
Dataflow.reduce_epoch(
    reducer: Callable[[Any, Any], Any],
)
```

**Reduce epoch** lets you combine all items for a key within an epoch into an aggregator.

This is like [reduce](#reduce) but marks the aggregator as complete automatically at the end of each epoch.

Since this is a stateful operator, it requires the the input stream has items that are `(key, value)` tuples so we can ensure that all relevant values are routed to the relevant aggregator.
Use [map](#map) beforehand to extract out the key.

It calls a function `reducer(aggregator: Any, value: Any) => updated_aggregator: Any` which combines two values. The aggregator is initially the first value seen for a key. Values will be passed in arbitrary order.

It emits `(key, aggregator)` tuples downstream at the end of each epoch.

```python doctest:SORT_OUTPUT doctest:SORT_EXPECTED
def add_initial_count(event):
    return event["user"], 1


def count(count, event_count):
    return count + event_count


flow = Dataflow()
flow.map(add_initial_count)
flow.inspect_epoch(lambda epoch, item: print("Saw", item, "@", epoch))
flow.reduce_epoch(count)
flow.capture()


inp = [
    (0, {"user": "a", "type": "login"}),
    (0, {"user": "a", "type": "post"}),
    (0, {"user": "b", "type": "login"}),
    (1, {"user": "b", "type": "post"}),
]
for epoch, item in run(flow, inp):
    print(epoch, item)
```

```{testoutput}
Saw ('a', 1) @ 0
Saw ('b', 1) @ 0
Saw ('a', 1) @ 0
Saw ('b', 1) @ 1
0 ('b', 1)
0 ('a', 2)
1 ('b', 1)
```

### Examples

- [Word Count](https://github.com/bytewax/bytewax/blob/main/examples/word_count.py)
- [Wikistream](https://github.com/bytewax/bytewax/blob/main/examples/wikistream.py)
- [Twitter Stream](https://github.com/bytewax/bytewax/blob/main/examples/twitter_stream.py)
- [PageRank](https://github.com/bytewax/bytewax/blob/main/examples/pagerank.py)

### Common Use Cases

- Counting within epochs
- Aggregation within epochs

## Reduce Epoch Local

```
Dataflow.reduce_epoch_local(
    reducer: Callable[[Any, Any], Any],
)
```

**Reduce epoch local** lets you combine all items for a key within an epoch _on a single worker._

It is exactly like [reduce epoch](#reduce-epoch) but does not ensure all values for a key are routed to the same worker and thus there is only one output aggregator per key.
You should prefer [reduce epoch](#reduce-epoch) over this operator unless you need a network-overhead optimization and some later step does full aggregation.

### Examples

- [PageRank](https://github.com/bytewax/bytewax/blob/main/examples/pagerank.py)
- [Parquet](https://github.com/bytewax/bytewax/blob/main/examples/events_to_parquet.py)

### Common Use Cases

- Performance optimization

## Stateful Map

```
Dataflow.stateful_map(
    step_id: str,
    builder: Callable[[], Any],
    mapper: Callable[[Any, Any], Any],
)
```

**Stateful map** is a one-to-one transformation of values in `(key, value)` pairs, but allows you to reference a persistent state for each key when doing the transformation.

Since this is a stateful operator, it requires the the input stream has items that are `(key, value)` tuples so we can ensure that all relevant values are routed to the relevant state.

It calls two functions:

- A `builder() => new_state: Any` which returns a new state and will be called whenever a new key is encountered.

- A `mapper(state: Any, value: Any) => (updated_state: Any, updated_value: Any)` which transforms values.
Values will be passed in epoch order, but no order is defined within an epoch.
If the updated state is `None`, the state will be forgotten.

It emits a `(key, updated_value)` tuple downstream for each input item.

```python doctest:SORT_OUTPUT
def self_as_key(item):
    return item, item


def build_count(key):
    return 0


def check(running_count, item):
    running_count += 1
    if running_count == 1:
        return running_count, item
    else:
        return running_count, None


def remove_none_and_key(key_item):
    key, item = key_item
    if item is None:
        return []
    else:
        return [item]


flow = Dataflow()
flow.map(self_as_key)
flow.stateful_map("counter", build_count, check)
flow.flat_map(remove_none_and_key)
flow.capture()


inp = [
    (0, "a"),
    (0, "a"),
    (0, "a"),
    (1, "a"),
    (1, "b"),
]
for epoch, item in run(flow, inp):
    print(epoch, item)
```

```{testoutput}
0 a
1 b
```

### Examples

- [Anomaly Detector](https://github.com/bytewax/bytewax/blob/main/examples/anomaly_detector.py)

### Common Use Cases

- Anomaly detection
- State machines

# Design Patterns

Here are some tips, tricks, and Python features that solve common questions and help you write succinct, easy-to-read code.

## Quick Logic Functions

All of the above examples define named custom logic functions and then pass them to an operator.
Any callable value can be used as-is, though!

This means you can use the following existing callables to help you make code more concise:

- [Built-in functions](https://docs.python.org/3/library/functions.html)
- [Constructors](https://docs.python.org/3/tutorial/classes.html#class-objects)
- [Methods](https://docs.python.org/3/glossary.html#term-method)

You can also use [lambdas](https://docs.python.org/3/tutorial/controlflow.html#lambda-expressions) to quickly define one-off anonymous functions for simple custom logic.

The following sets of examples are equivalent.

For flat map:

```python
def split_sentence(sentence):
    return sentence.split()


flow = Dataflow()
flow.flat_map(split_sentence)
flow.capture()


inp = [(0, "hello world")]
print(run(flow, inp))
```

```{testoutput}
[(0, 'hello'), (0, 'world')]
```

```python
flow = Dataflow()
flow.flat_map(lambda s: s.split())
flow.capture()


inp = [(0, "hello world")]
print(run(flow, inp))
```

```{testoutput}
[(0, 'hello'), (0, 'world')]
```

```python
flow = Dataflow()
flow.flat_map(str.split)
flow.capture()


inp = [(0, "hello world")]
print(run(flow, inp))
```

```{testoutput}
[(0, 'hello'), (0, 'world')]
```

For inspect epoch:

```python
def log(epoch, item):
    print(epoch, item)


flow = Dataflow()
flow.inspect_epoch(log)
flow.capture()


inp = [(0, "a"), (1, "b")]
run(flow, inp)  # Note no print here.
```

```{testoutput}
0 a
1 b
```

```python
flow = Dataflow()
flow.inspect_epoch(lambda e, i: print(e, i))
flow.capture()


inp = [(0, "a"), (1, "b")]
run(flow, inp)
```

```{testoutput}
0 a
1 b
```

```python
flow = Dataflow()
flow.inspect_epoch(print)
flow.capture()


inp = [(0, "a"), (1, "b")]
run(flow, inp)
```

```{testoutput}
0 a
1 b
```

For reduce epoch:

```python
def add_to_list(l, items):
    l.extend(items)
    return l


flow = Dataflow()
flow.reduce_epoch(add_to_list)
flow.capture()


inp = [(0, ("a", ["x"])), (0, ("a", ["y"]))]
print(run(flow, inp))
```

```{testoutput}
[(0, ('a', ['x', 'y']))]
```

```python
flow = Dataflow()
flow.reduce_epoch(lambda l1, l2: l1 + l2)
flow.capture()


inp = [(0, ("a", ["x"])), (0, ("a", ["y"]))]
print(run(flow, inp))
```

```{testoutput}
[(0, ('a', ['x', 'y']))]
```

```python
import operator


flow = Dataflow()
flow.reduce_epoch(operator.add)
flow.capture()


inp = [(0, ("a", ["x"])), (0, ("a", ["y"]))]
print(run(flow, inp))
```

```{testoutput}
[(0, ('a', ['x', 'y']))]
```

For filter:

```python
def is_odd(item):
    return item % 2 == 1


flow = Dataflow()
flow.filter(is_odd)
flow.capture()


inp = [(0, 5), (0, 9), (0, 2)]
print(run(flow, inp))
```

```{testoutput}
[(0, 5), (0, 9)]
```

```python
flow = Dataflow()
flow.filter(lambda x: x % 2 == 1)
flow.capture()


inp = [(0, 5), (0, 9), (0, 2)]
print(run(flow, inp))
```

```{testoutput}
[(0, 5), (0, 9)]
```

## Subflows

If you find yourself repeating a series of steps in your dataflows or want to give some steps a descriptive name, you can group those steps into a **subflow** function which adds a sequence of steps.
You can then call that subflow function whenever you need that step sequence.
This is just calling a function.

```python
def user_reducer(all_events, new_events):
    return all_events + new_events


def collect_user_events(flow):
    # event
    flow.map(lambda e: (e["user_id"], [e]))
    # (user_id, event)
    flow.reduce_epoch(user_reducer)
    # (user_id, events_for_user)
    flow.map(lambda u_es: u_es[1])
    # events_for_user


flow = Dataflow()
collect_user_events(flow)
flow.capture()


inp = [
    (0, {"user_id": 1, "type": "login"}),
    (0, {"user_id": 1, "type": "logout"}),
]
print(run(flow, inp))
```

```{testoutput}
[(0, [{'user_id': 1, 'type': 'login'}, {'user_id': 1, 'type': 'logout'}])]
```

### Examples

- [Subflow](https://github.com/bytewax/bytewax/blob/main/examples/subflow.py)
