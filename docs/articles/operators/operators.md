In this section, we'll cover the Operators that are available in bytewax, and how to use them.

**Operators** are the processing primitives of bytewax.
Each of them gives you a "shape" of data transformation, and you give them functions to customize them to a specific task you need.
The combination of each operator and their custom logic functions we call a dataflow **step**.
You chain together steps in a dataflow to solve your high-level data processing problem.

You can add steps to your dataflow by calling the method with the name of the operator you'd like to use on the `Dataflow` object.

If you've ever used Python's [`map`](https://docs.python.org/3/library/functions.html#map) or [`filter`](https://docs.python.org/3/library/functions.html#filter) or `functool`'s [`reduce`](https://docs.python.org/3/library/functools.html#functools.reduce) or equivalent in other languages, operators are the same concept.
If not, no worries, we'll give a quick overview of each and links to our annotated examples which demonstrate the use of each in a relevant way.

# List of Operators

## Inspect

```python
Dataflow.inspect(inspector: Callable[[Any], None])
```

**Inspect** allows you to observe, but not modify, items.

It calls a function `inspector(item: Any) => None` on each item.

The return value is ignored; it emits items downstream unmodified.

```python
def log(item):
    print(f"Saw {item}")

flow = ec.Dataflow(enumerate(range(3)))
flow.inspect(log)
# Saw 1
# Saw 2
# Saw 3
```

### Examples

Inspect is pervasive and is in almost every example as a debugging tool.

### Common Use Cases

- Debugging
- Logging

## Map

```python
Dataflow.map(mapper: Callable[[Any], Any])
```

**Map** is a one-to-one transformation of items.

It calls a function `mapper(item: Any) => transformed_x: Any` on each item.

It emits each transformed item downstream.

```python
def add_one(item):
    return item + 1

flow = ec.Dataflow(enumerate(range(3)))
flow.map(add_one)
flow.inspect(print)
# 2
# 1
# 3
```

### Examples

Map is pervasive and in almost every example, but here are our most simple examples.

- [Basic](https://github.com/bytewax/bytewax/blob/main/examples/basic.py)
- [Translator](https://github.com/bytewax/bytewax/blob/main/examples/translator.py)

### Common Use Cases

- Turning JSON into objects for processing
- Extracting a key from a compound object

## Filter

```python
Dataflow.filter(predicate: Callable[[Any], bool])
```

**Filter** selectively keeps only some items.

It calls a function `predicate(item: Any) => should_emit: bool` on each item.

It emits the item downstream unmodified if the predicate returns `True`.

```python
def is_odd(item):
    return item % 2 != 0

flow = ec.Dataflow(enumerate(range(3)))
flow.filter(is_odd)
flow.inspect(print)
# 3
# 1
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

```python
Dataflow.flat_map(mapper: Callable[[Any], Iterable[Any]])
```

**Flat map** is a one-to-many transformation of items.

It calls a function `mapper(item: Any) => emit: Iterable[Any]` on each item.

It emits each element in the downstream iterator individually.

```python
def split_into_words(sentence):
    return sentence.split()

flow = ec.Dataflow(enumerate(["hello world"]))
flow.flat_map(split_into_words)
flow.inspect(print)
# world
# hello
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

```python
Dataflow.inspect_epoch(inspector: Callable[[int, Any], None])
```

**Inspect epoch** allows you to observe, but not modify, items and their epochs.

It calls a function `inspector(epoch: int, item: Any) => None` on each item with its epoch.

The return value is ignored; it emits items downstream unmodified.

```python
def log(epoch, item):
    print(f"Saw {item} @ {epoch}")

flow = ec.Dataflow(enumerate(range(3)))
flow.inspect_epoch(log)
# Saw 1 @ 1
# Saw 2 @ 2
# Saw 3 @ 3
```

### Examples

Inspect epoch is pervasive and used in many examples as a debugging tool.

### Common Use Cases

- Debugging
- Logging

## Capture

```python
Dataflow.capture(captor: Callable[[int, Any], None])
```

**Capture** allows you to observe all items and their epochs that pass through a point on the dataflow _from all workers_.

It calls a function `captor(epoch_item: tuple[int, Any]) => None` on each item of data.
There are no ordering guarantees; this function can be called in any epoch or item order.

Because capture allows you to observe and collect all data on all workers, it has larger overhead than other operators.

The return value is ignored; it emits items downstream unmodified.

```python
out = []

def append(epoch_item):
    epoch, item = epoch_item
    out.append(item)

flow = ec.Dataflow(enumerate(range(3)))
flow.capture(append)
assert sorted(out) == sorted([1, 2, 3])
```

### Examples

Capture is used in all of our dataflow unit tests.

### Common Use Cases

- Unit testing
- Using a dataflow as part of a Jupyter notebook
- Incorporating an isolated bytewax dataflow as part of a larger program like a webserver

## Reduce

```python
Dataflow.reduce(
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

```python
def user_as_key(event):
    return (event["user"], [event])
    
def extend_session(session, events):
    session.extend(event)
    return session
    
def session_complete(session):
    return any(event["type"] == "logout" for event in session)

flow = ec.Dataflow([
    (0, {"user": "a", "type": "login"}),
    (1, {"user": "a", "type": "post"}),
    (1, {"user": "b", "type": "login"}),
    (2, {"user": "a", "type": "logout"}),
    (3, {"user": "b", "type": "logout"}),
])
flow.map(user_as_key)
flow.inspect_epoch(print)
# 0 ("a", [{"user": "a", "type": "login"}]),
# 1 ("b", [{"user": "b", "type": "login"}]),
# 1 ("a", [{"user": "a", "type": "post"}]),
# 2 ("a", [{"user": "a", "type": "logout"}]),
# 3 ("b", [{"user": "b", "type": "logout"}]),
flow.reduce(extend_session, session_complete)
flow.inspect_epoch(print)
# 2 ("a", [{"user": "a", "type": "login"}, {"user": "a", "type": "post"}, {"user": "a", "type": "logout"}])
# 3 ("b", [{"user": "b", "type": "login"}, {"user": "b", "type": "logout"}])
```

### Examples

- [Search Session](https://github.com/bytewax/bytewax/blob/main/examples/search_session.py)

### Common Use Cases

- Sessionization
- Emitting a summary of data that spans timestamps

## Reduce Epoch

```python
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

```python
def add_initial_count(event):
    return event["user"], 1
    
def count(count, event_count):
    return count + event_count

flow = ec.Dataflow([
    (0, {"user": "a", "type": "login"}),
    (0, {"user": "a", "type": "post"}),
    (0, {"user": "b", "type": "login"}),
    (1, {"user": "b", "type": "post"}),
])
flow.map(add_initial_count)
flow.inspect_epoch(print)
# (0, ("a", 1))
# (0, ("b", 1))
# (0, ("a", 1))
# (1, ("b", 1))
flow.reduce_epoch(build_counter, count)
flow.inspect_epoch(print)
# (0, ("b", 1))
# (0, ("a", 2))
# (1, ("b", 1))
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

```python
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

```python
Dataflow.stateful_map(
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

```python
def build_seen():
    return set()
    
def check(seen, item):
    if item in seen:
        return seen, None
    else:
        seen.add(item)
        return seen, item

def remove_none(item):
    return item is not None

flow = ec.Dataflow([
    (0, "a"),
    (0, "a"),
    (0, "a"),
    (1, "a"),
    (1, "b"),
])
flow.stateful_map(build_counter, count)
flow.filter(remove_none)
flow.inspect_epoch(print)
# 0 "a"
# 1 "b"
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

```python
def split_sentence(sentence):
    return sentence.split()
    
flow.flat_map(split_sentence)

---

flow.flat_map(lambda s: s.split())

---

flow.flat_map(str.split)
```

```python
def log(epoch, item):
    print(epoch, item)
    
flow.inspect_epoch(log)

---

flow.inspect_epoch(lambda e, x: print(e, x))

---

flow.inspect_epoch(print)
```

```python
def add_to_list(l, items):
    l.extend(items)
    return l
    
flow.reduce_epoch(add_to_list)

---

flow.reduce_epoch(lambda l1, l2: l1 + l2)

---
import operator

flow.reduce_epoch(operator.add)
```

```python
def is_odd(item):
    return item % 2 == 1
    
flow.filter(is_odd)

---

flow.filter(lambda x: x % 2)
```

## Subflows

If you find yourself repeating a series of steps in your dataflows or want to give some steps a descriptive name, you can group those steps into a **subflow** function which adds a sequence of steps.
You can then call that subflow function whenever you need that step sequence.
This is just calling a function.

```python
def user_reducer(l, user, events):
    return l.extend(events)

def collect_user_events(flow):
    # event
    flow.map(lambda e: (e.user, e))
    # (user, event)
    flow.aggregate(list, user_reducer)
    # (user, events_for_user)
    flow.map(lambda u_es: u_es[1])
    # events_for_user

collect_user_events(flow)
```

### Examples

- [Subflow](https://github.com/bytewax/bytewax/blob/main/examples/subflow.py)
