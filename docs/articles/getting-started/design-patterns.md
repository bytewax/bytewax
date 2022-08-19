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
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig


def split_sentence(sentence):
    return sentence.split()


# We'll populate this list with the output so we can inspect it
result = []
output = TestingOutputConfig(result)
input = TestingInputConfig(["hello world"])

flow = Dataflow()
flow.input(input)
# This is where we operate on the input
flow.flat_map(split_sentence)
flow.capture(output)

run_main(flow)
# Print the result list
print(result)
```

```{testoutput}
['hello', 'world']
```

```python
# ...
# This is where we operate on the input
flow.flat_map(lambda s: s.split())
# ...
```

```{testoutput}
['hello', 'world']
```

```python
# ...
# This is where we operate on the input
flow.flat_map(str.split)
# ...
```

```{testoutput}
['hello', 'world']
```

For inspect epoch:

```python
def log(epoch, item):
    print(epoch, item)

input = TestingInputConfig(["a", "b"])
output = TestingOutputConfig([])

flow = Dataflow()
flow.input(input)
flow.inspect_epoch(log)
flow.capture(output)

run_main(flow)
# Note no print here.
```

```{testoutput}
0 a
0 b
```

```python
flow = Dataflow()
flow.input(input)
flow.inspect_epoch(lambda e, i: print(e, i))
flow.capture(output)

run_main(flow)
```

```{testoutput}
0 a
0 b
```

```python
flow = Dataflow()
flow.input(input)
flow.inspect_epoch(print)
flow.capture(output)

run_main(flow)
```

```{testoutput}
0 a
0 b
```

For reduce window:

```python
def add_to_list(l, items):
    l.extend(items)
    return l


result = []
input = TestingInputConfig([("a", ["x"]), ("a", ["y"])])
output = TestingOutputConfig(result)

cc = TestingClockConfig(item_incr=timedelta(seconds=1))
wc = TumblingWindowConfig(length=timedelta(seconds=2))

flow = Dataflow()
flow.input(input)
# This is where we operate on the input
flow.reduce_window("reduce", cc, wc, add_to_list)
flow.capture(output)

run_main(flow)
print(result)
```

```{testoutput}
[('a', ['x', 'y'])]
```

```python
# ...
# This is where we operate on the input
flow.reduce_window("reduce", cc, wc, lambda l1, l2: l1 + l2)
# ...
```

```{testoutput}
[('a', ['x', 'y'])]
```

```python
import operator
# ...
# This is where we operate on the input
flow.reduce_window("reduce", cc, wc, operator.add)
# ...
```

```{testoutput}
[('a', ['x', 'y'])]
```

For filter:

```python
def is_odd(item):
    return item % 2 == 1

result = []
input = TestingInputConfig([5, 9, 2])
output = TestingOutputConfig(result)

flow = Dataflow()
flow.input(input)
# This is where we operate on the input
flow.filter(is_odd)
flow.capture(output)

run_main(flow)
print(result)
```

```{testoutput}
[5, 9]
```

```python
# ...
# This is where we operate on the input
flow.filter(lambda x: x % 2 == 1)
# ...
```

```{testoutput}
[5, 9]
```

## Subflows

If you find yourself repeating a series of steps in your dataflows or want to give some steps a descriptive name, you can group those steps into a **subflow** function which adds a sequence of steps.
You can then call that subflow function whenever you need that step sequence.
This is just calling a function.

```python
def user_reducer(all_events, new_events):
    return all_events + new_events


def collect_user_events(flow, cc, wc):
    # event
    flow.map(lambda e: (e["user_id"], [e]))
    # (user_id, event)
    flow.reduce_window("reducer", cc, wc, user_reducer)
    # (user_id, events_for_user)
    flow.map(lambda u_es: u_es[1])
    # events_for_user


result = []
input = TestingInputConfig(
    [{"user_id": "1", "type": "login"}, {"user_id": "1", "type": "logout"}]
)
output = TestingOutputConfig(result)
cc = TestingClockConfig(item_incr=timedelta(seconds=1))
wc = TumblingWindowConfig(length=timedelta(seconds=2))

flow = Dataflow(input)
collect_user_events(flow, cc, wc)
flow.capture(output)

run_main(flow)
print(result)
```

```{testoutput}
[[{'user_id': '1', 'type': 'login'}, {'user_id': '1', 'type': 'logout'}]]
```
