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
from bytewax import Dataflow, run


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
    (0, {"user_id": "1", "type": "login"}),
    (0, {"user_id": "1", "type": "logout"}),
]
print(run(flow, inp))
```

```{testoutput}
[(0, [{'user_id': '1', 'type': 'login'}, {'user_id': '1', 'type': 'logout'}])]
```
