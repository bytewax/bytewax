Dataflow programming is a programming paradigm where program execution
is conceptualized as data flowing through a static series of
operations.

A Bytewax **dataflow** is a fixed directed acyclic graph of
computational **steps** which are preformed on a possibly-unbounded
stream of data. Each step is made up of an **operator** or a specific
shape of computation (e.g. "transform each item individually" /
[`map`](/apidocs/bytewax.operators/index#bytewax.operators.map)). You
define a dataflow via Python code and run the dataflow, and it goes
out and polls the input for new items and automatically pushes them
through the steps until they reach an output. An **item** is a single
Python object that is flowing through the dataflow.

See [our getting started guides](../getting-started/simple-example.md)
for the most basic examples. In this document, we're going to discuss
in more detail the conceptual parts of dataflow programming.

### Operators and Logic Functions

**Operators** are the processing primitives of Bytewax. Each of them
gives you a "shape" of data transformation, and you give them **logic
functions** to customize them to a specific task you need. The
combination of each operator and their custom logic functions we call
a dataflow **step**. You chain together steps in a dataflow to solve
your high-level data processing problem.

If you've ever used Python's built-in functions
[`map`](https://docs.python.org/3/library/functions.html#map) or
[`filter`](https://docs.python.org/3/library/functions.html#filter) or
`functool`'s
[`reduce`](https://docs.python.org/3/library/functools.html#functools.reduce)
or equivalent in other languages, operators are the same concept. If
not, no worries, there's an example provided in the documentation for
each operator in
[`bytewax.operators`](/apidocs/bytewax.operators/index).

To help you understand the concept, let's walk through two basic
Bytewax operators:
[`flat_map`](/apidocs/bytewax.operators/index#bytewax.operators.flat_map)
and
[`stateful_map`](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map).

#### Flat Map Example

[`flat_map`](/apidocs/bytewax.operators/index#bytewax.operators.flat_map)
is an operator that applies a mapper function to each upstream item
and lets you return a list of items to individually emit into the
downstream. Here's a quick example that splits a stream of sentences
into a stream of individual words.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("flat_map_eg")
inp = op.input("inp", flow, TestingSource(["hello world", "how are you"]))
op.inspect("check_inp", inp)


def split_sentence(sentence):
    return sentence.split()


split = op.flat_map("split", inp, split_sentence)
op.inspect("check_split", split)
run_main(flow)
```

Note how the `split` stream has flattened the lists returned from the
mapper logic function.

```{testoutput}
flat_map_eg.check_inp: 'hello world'
flat_map_eg.check_split: 'hello'
flat_map_eg.check_split: 'world'
flat_map_eg.check_inp: 'how are you'
flat_map_eg.check_split: 'how'
flat_map_eg.check_split: 'are'
flat_map_eg.check_split: 'you'
```

There's nothing special about a logic function. It is a normal Python
function and you can call it directly to test or debug its behavior.

```python
print(split_sentence("so many words here"))
```

```{testoutput}
['so', 'many', 'words', 'here']
```

The important part of the logic function is that it's signature
matches what the operator requires. For example, the function
signature for
[`flat_map`](/apidocs/bytewax.operators/index#bytewax.operators.flat_map)
has `mapper: Callable[[~X], Iterable[~Y]]`, which means the `mapper`
must be a function that takes a single argument and returns an
iterator.

#### Step IDs

Each operator function takes as a first argument a **step ID** you
should set to a dataflow-unique string that represents the purpose of
this computational step. This gets attached to errors, logging, and
metrics so you can link these bits of information for debugging. It
also gets attached to state snapshots so that Bytewax properly
recovers data.

Because operators are hierarchical and made up of collections of
**sub-operators**, you might see step IDs in error messages that look
like dotted paths, e.g. in the above dataflow
`flat_map_eg.split.flat_map_batch` exists. In general, find your
operator in the path and look for trouble there, so in this example
the fact `split` is in the path means there might be an issue with the
logic function or operator there.

### Streams and Directed Acyclic Dataflows

Each operator takes and returns one or more
[`Stream`](/apidocs/bytewax.dataflow/index#bytewax.dataflow.Stream)s.
Each **stream** is a handle to that specific flow of items. The
streams that are arguments to operators we call **upstreams** and
returns some streams we call **downstreams**.

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

flow = Dataflow("copied_math")
nums = op.input("nums", flow, TestingSource([1, 2, 3]))
op.inspect("check_nums", nums)
doubles = op.map("do_double", nums, lambda x: x * 2)
op.inspect("check_doubles", doubles)
tens = op.map("do_tens", nums, lambda x: x * 10)
op.inspect("check_tens", tens)
all = op.merge("merge", doubles, tens)
op.inspect("check_merge", all)
run_main(flow)
```

We added a ton of
[`inspect`](/apidocs/bytewax.operators/index#bytewax.operators.inspect)
steps so we can see the items flowing through every point in this
dataflow.

```{testoutput}
copied_math.check_nums: 1
copied_math.check_doubles: 2
copied_math.check_tens: 10
copied_math.check_merge: 2
copied_math.check_merge: 10
copied_math.check_nums: 2
copied_math.check_doubles: 4
copied_math.check_tens: 20
copied_math.check_merge: 4
copied_math.check_merge: 20
copied_math.check_nums: 3
copied_math.check_doubles: 6
copied_math.check_tens: 30
copied_math.check_merge: 6
copied_math.check_merge: 30
```

If you'd like to take a stream and selectively send the items within
down one of two streams (instead of copying all of them), you can use
the
[`branch`](/apidocs/bytewax.operators/index#bytewax.operators.branch)
operator.

```python
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("copied_math")
nums = op.input("nums", flow, TestingSource([1, 2, 3]))
op.inspect("check_nums", nums)
branch_out = op.branch("branch_even", nums, lambda x: x % 2 == 0)
evens = branch_out.trues
op.inspect("check_evens", evens)
odds = branch_out.falses
op.inspect("check_odds", odds)
halves = op.map("do_half", evens, lambda x: x / 2)
op.inspect("check_halves", halves)
all = op.merge("merge", halves, odds)
op.inspect("check_merge", all)
run_main(flow)
```

```{testoutput}
copied_math.check_nums: 1
copied_math.check_odds: 1
copied_math.check_merge: 1
copied_math.check_nums: 2
copied_math.check_evens: 2
copied_math.check_halves: 1.0
copied_math.check_merge: 1.0
copied_math.check_nums: 3
copied_math.check_odds: 3
copied_math.check_merge: 3
```

Because references to
[`Stream`](/apidocs/bytewax.dataflow/index#bytewax.dataflow.Stream)s
are only created via operators, that means you can't reference a
stream before it is created and thus the resulting dataflow is always
a directed acyclic graph.

### State

One of the major selling points of Bytewax and stateful stream
processing is the ability to incrementally modify persistent state as
you process the stream (think calculating moving averages, maximums,
joining together data, e.g.) while giving you a runtime that helps you
handle restarts, crashes, and rescaling of the computational resources
backing the dataflow.

These advanced powers do not come for free! Bytewax is great in that
it uses plain Python and any packages you import, but it requires you
to play by some specific rules in order for the dataflows you write to
behave properly in a cluster environment over restarts and rescaling.
We'll go over the constraints here.

- Global Python objects can be loaded from anywhere and closed over in
  logic functions, but you _must not mutate it_.

- All state that is modified _must be managed by stateful operators_.
  We'll discuss those below.

- Writing to or persistent monitoring of external data stores _must go
  through IO operators / connectors_.

(There are reasons to violate these constraints, but the ramifications
of doing so required a good understanding of the Bytewax runtime and
recovery system and is out of the scope of this guide.)

#### Global Static State

If you have a dataset that you know is immutable (or are OK with
ignoring ongoing updates on it), you can reference that state within
any logic functions and use it. This can be an easy way to implement a
"static join".

In this example, we turn the `"avatar_icon_code"` into an URL using a
static lookup table.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

ICON_TO_URL = {
    "dog_ico": "http://domain.invalid/static/dog_v1.png",
    "cat_ico": "http://domain.invalid/static/cat_v2.png",
    "rabbit_ico": "http://domain.invalid/static/rabbit_v1.png",
}

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {"user_id": "1", "avatar_icon_code": "dog_ico"},
            {"user_id": "3", "avatar_icon_code": "rabbit_ico"},
            {"user_id": "2", "avatar_icon_code": "dog_ico"},
        ]
    ),
)
op.inspect("check_inp", inp)


def icon_code_to_url(msg):
    code = msg.pop("avatar_icon_code")
    msg["avatar_icon_url"] = ICON_TO_URL[code]
    return msg


with_urls = op.map("with_url", inp, icon_code_to_url)
op.inspect("check_with_url", with_urls)
run_main(flow)
```

```{testoutput}
param_eg.check_inp: {'user_id': '1', 'avatar_icon_code': 'dog_ico'}
param_eg.check_with_url: {'user_id': '1', 'avatar_icon_url': 'http://domain.invalid/static/dog_v1.png'}
param_eg.check_inp: {'user_id': '3', 'avatar_icon_code': 'rabbit_ico'}
param_eg.check_with_url: {'user_id': '3', 'avatar_icon_url': 'http://domain.invalid/static/rabbit_v1.png'}
param_eg.check_inp: {'user_id': '2', 'avatar_icon_code': 'dog_ico'}
param_eg.check_with_url: {'user_id': '2', 'avatar_icon_url': 'http://domain.invalid/static/dog_v1.png'}
```

You can also call out to an external service or API or data store in a
[`map`](/apidocs/bytewax.operators/index#bytewax.operators.map) step
in the same way. But be careful that you are assuming the external
data is static and you will not be emitting updates if it changes
after the call.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main


def query_icon_url_service(code):
    # This static code is just for show in this example to make it run-able. You instead would do something like:
    # return requests.get(f"http://internal-url-service.invalid/avatar?code={code}").json()
    if code == "dog_ico":
        return "http://domain.invalid/static/dog_v1.png"
    elif code == "cat_ico":
        return "http://domain.invalid/static/cat_v2.png"
    elif code == "rabbit_ico":
        return "http://domain.invalid/static/rabbit_v1.png"


flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {"user_id": "1", "avatar_icon_code": "dog_ico"},
            {"user_id": "3", "avatar_icon_code": "rabbit_ico"},
            {"user_id": "2", "avatar_icon_code": "dog_ico"},
        ]
    ),
)
op.inspect("check_inp", inp)


def icon_code_to_url(msg):
    code = msg.pop("avatar_icon_code")
    msg["avatar_icon_url"] = query_icon_url_service(code)
    return msg


with_urls = op.map("with_url", inp, icon_code_to_url)
op.inspect("check_with_url", with_urls)
run_main(flow)
```

```{testoutput}
param_eg.check_inp: {'user_id': '1', 'avatar_icon_code': 'dog_ico'}
param_eg.check_with_url: {'user_id': '1', 'avatar_icon_url': 'http://domain.invalid/static/dog_v1.png'}
param_eg.check_inp: {'user_id': '3', 'avatar_icon_code': 'rabbit_ico'}
param_eg.check_with_url: {'user_id': '3', 'avatar_icon_url': 'http://domain.invalid/static/rabbit_v1.png'}
param_eg.check_inp: {'user_id': '2', 'avatar_icon_code': 'dog_ico'}
param_eg.check_with_url: {'user_id': '2', 'avatar_icon_url': 'http://domain.invalid/static/dog_v1.png'}
```

If you do want to monitor an external data source for changes, you'll
want to find / make a connector that can introduce its change stream
into the dataflow and join it. See our [joins concepts
documentation](joins.md) for more info.

#### Stateful Operators

To manage any mutable state, it must be within a **stateful operator**
are any operators which bring together data from multiple items. E.g.
[`stateful_map`](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map),
[`join`](/apidocs/bytewax.operators/index#bytewax.operators.join), all
the window operators like
[`collect_window`](/apidocs/bytewax.operators.window/index#bytewax.operators.window.collect_window).

##### State Keys

The Bytewax runtime enables parallelization of stateful operators by
requiring the incoming data to have a state key. A **state key** is a
string which partitions the state in the operator. All items with the
same key operate on the same isolated state. There is no interaction
between state or items with different keys. This is required because
the Bytewax runtime shards state among the worker processes; only a
single worker handles the state for each key, so it can apply changes
in serial.

What you should use for the state key depends on specifically what you
want to do in your stateful step. In general it will be things like
"user ID" or "session ID". Make this key unique enough so you don't
bring together too much, but be weary using a static constant, since
it will bring together all items from the whole dataflow into one
worker.

This key must be a string, so if you have a different data type,
you'll need to convert that type to a string. The
[`key_on`](/apidocs/bytewax.operators/index#bytewax.operators.key_on)
and [`map`](/apidocs/bytewax.operators/index#bytewax.operators.map)
operators can help you with this.

##### Stateful Map Example

Let's demonstrate these concepts with an example using
[`stateful_map`](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map).
It performs a transformation on each upstream item, allowing reference
to a persistent state. That persistent state is passed as the first
argument to the logic function and the function must return it as the
first return value.

Let's calculate the running mean of the last 3 transactions for a
user.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

src_items = [
    {"user_id": 1, "txn_amount": 100.0},
    {"user_id": 1, "txn_amount": 17.0},
    {"user_id": 2, "txn_amount": 30.0},
    {"user_id": 2, "txn_amount": 5.0},
    {"user_id": 1, "txn_amount": 120.0},
    {"user_id": 2, "txn_amount": 45.0},
    {"user_id": 1, "txn_amount": 99.0},
]

flow = Dataflow("stateful_map_eg")
inp = op.input("inp", flow, TestingSource(src_items))
op.inspect("check_inp", inp)
```

First, since we're calculating the running mean per-user, we should
use the user ID as the key because there will be no interaction. Let's
pluck out the user ID using
[`key_on`](/apidocs/bytewax.operators/index#bytewax.operators.key_on)
and use a logic function that picks the user ID field and casts to a
string.

```python
keyed_inp = op.key_on("key", inp, lambda msg: str(msg["user_id"]))
op.inspect("check_keyed", keyed_inp)
```

Let's also get rid of the `dict` data structure and unpack the
transaction amount directly to make downstream steps simpler. The
[`map_value`](/apidocs/bytewax.operators/index#bytewax.operators.map_value)
is a convenience function that maps just the `value` part of `(key,
value)` 2-tuples. Since we just added a key, this makes this function
less tricky.

```python
keyed_amounts = op.map_value("pick_amount", keyed_inp, lambda msg: msg["txn_amount"])
op.inspect("check_keyed_amount", keyed_amounts)
```

Let's inspect the steps we've made thus far to see the
transformations: add a user ID key, unpack the dictionary.

```python
run_main(flow)
```

```{testoutput}
stateful_map_eg.check_inp: {'user_id': 1, 'txn_amount': 100.0}
stateful_map_eg.check_keyed: ('1', {'user_id': 1, 'txn_amount': 100.0})
stateful_map_eg.check_keyed_amount: ('1', 100.0)
stateful_map_eg.check_inp: {'user_id': 1, 'txn_amount': 17.0}
stateful_map_eg.check_keyed: ('1', {'user_id': 1, 'txn_amount': 17.0})
stateful_map_eg.check_keyed_amount: ('1', 17.0)
stateful_map_eg.check_inp: {'user_id': 2, 'txn_amount': 30.0}
stateful_map_eg.check_keyed: ('2', {'user_id': 2, 'txn_amount': 30.0})
stateful_map_eg.check_keyed_amount: ('2', 30.0)
stateful_map_eg.check_inp: {'user_id': 2, 'txn_amount': 5.0}
stateful_map_eg.check_keyed: ('2', {'user_id': 2, 'txn_amount': 5.0})
stateful_map_eg.check_keyed_amount: ('2', 5.0)
stateful_map_eg.check_inp: {'user_id': 1, 'txn_amount': 120.0}
stateful_map_eg.check_keyed: ('1', {'user_id': 1, 'txn_amount': 120.0})
stateful_map_eg.check_keyed_amount: ('1', 120.0)
stateful_map_eg.check_inp: {'user_id': 2, 'txn_amount': 45.0}
stateful_map_eg.check_keyed: ('2', {'user_id': 2, 'txn_amount': 45.0})
stateful_map_eg.check_keyed_amount: ('2', 45.0)
stateful_map_eg.check_inp: {'user_id': 1, 'txn_amount': 99.0}
stateful_map_eg.check_keyed: ('1', {'user_id': 1, 'txn_amount': 99.0})
stateful_map_eg.check_keyed_amount: ('1', 99.0)
```

Now that we've done all the prep work to key the stream and prepare
the values, let's actually write out the running mean calculating
operator using
[`stateful_map`](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map).

```python
flow = Dataflow("stateful_map_eg")
inp = op.input("inp", flow, TestingSource(src_items))
keyed_inp = op.key_on("key", inp, lambda msg: str(msg["user_id"]))
keyed_amounts = op.map_value("pick_amount", keyed_inp, lambda msg: msg["txn_amount"])


def running_builder():
    return []


def calc_running_mean(values, new_value):
    values.append(new_value)
    while len(values) > 3:
        values.pop(0)

    running_mean = sum(values) / len(values)
    return (values, running_mean)


running_means = op.stateful_map(
    "running_mean", keyed_amounts, running_builder, calc_running_mean
)
op.inspect("check_running_mean", running_means)
run_main(flow)
```

[`stateful_map`](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map)
takes two logic functions:

- A builder which builds the initial "empty" state for a key. In our
  case, we want the empty list because we're storing the three most
  recent items.

- A mapper which takes the previous state (or the just-built state)
  and a new incoming value, and returns a 2-tuple of the
  `(updated_state, emit_value)`. In our case, we want to add the new
  item to the running list, remove any old items if it's too long,
  then calculate the running mean.

```{testoutput}
stateful_map_eg.check_running_mean: ('1', 100.0)
stateful_map_eg.check_running_mean: ('1', 58.5)
stateful_map_eg.check_running_mean: ('2', 30.0)
stateful_map_eg.check_running_mean: ('2', 17.5)
stateful_map_eg.check_running_mean: ('1', 79.0)
stateful_map_eg.check_running_mean: ('2', 26.666666666666668)
stateful_map_eg.check_running_mean: ('1', 78.66666666666667)
```

Is this creating the correct result? What's happening here? Let's
modify our mapper function slightly so we can watch the evolution of
the state. Instead of just emitting the running mean, print out the
state changes as they're happening.

```python
flow = Dataflow("stateful_map_eg")
inp = op.input("inp", flow, TestingSource(src_items))
keyed_inp = op.key_on("key", inp, lambda msg: str(msg["user_id"]))
keyed_amounts = op.map_value("pick_amount", keyed_inp, lambda msg: msg["txn_amount"])


def running_builder():
    return []


def calc_running_mean(values, new_value):
    print("Before state:", values)

    print("New value:", new_value)
    values.append(new_value)
    while len(values) > 3:
        values.pop(0)

    print("After state:", values)

    running_mean = sum(values) / len(values)
    print("Running mean:", running_mean)
    print()
    return (values, running_mean)


running_means = op.stateful_map(
    "running_mean", keyed_amounts, running_builder, calc_running_mean
)
op.inspect("check_running_mean", running_means)
run_main(flow)
```

```{testoutput}
Before state: []
New value: 100.0
After state: [100.0]
Running mean: 100.0

stateful_map_eg.check_running_mean: ('1', 100.0)
Before state: [100.0]
New value: 17.0
After state: [100.0, 17.0]
Running mean: 58.5

stateful_map_eg.check_running_mean: ('1', 58.5)
Before state: []
New value: 30.0
After state: [30.0]
Running mean: 30.0

stateful_map_eg.check_running_mean: ('2', 30.0)
Before state: [30.0]
New value: 5.0
After state: [30.0, 5.0]
Running mean: 17.5

stateful_map_eg.check_running_mean: ('2', 17.5)
Before state: [100.0, 17.0]
New value: 120.0
After state: [100.0, 17.0, 120.0]
Running mean: 79.0

stateful_map_eg.check_running_mean: ('1', 79.0)
Before state: [30.0, 5.0]
New value: 45.0
After state: [30.0, 5.0, 45.0]
Running mean: 26.666666666666668

stateful_map_eg.check_running_mean: ('2', 26.666666666666668)
Before state: [100.0, 17.0, 120.0]
New value: 99.0
After state: [17.0, 120.0, 99.0]
Running mean: 78.66666666666667

stateful_map_eg.check_running_mean: ('1', 78.66666666666667)
```

In that first set, we can see that the `running_builder` gave us an
empty list to start, we got the first value `100.0` and then it added
that to the list. In the second set, we can see how the second value
of `17.0` was added and how that changed the state that was used to
calculate the mean.

In the last set, we can see how the fourth value for user `1` caused
the state to bump out that first value of `100.0` and add `99.0` on
the end of the state but keeping it capped at three items.

## Helpful Patterns

### Quick Logic Functions

Operator's logic functions can be specified in a few ways. The most
verbose way would be to `def logic(...)` a function that does what you
need to do, but any callable value can be used as-is, though!

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
s = op.input("inp", flow, TestingSource(["hello world"]))


def split_sentence(sentence):
    return sentence.split()


s = op.flat_map("split", s, split_sentence)
op.inspect("out", s)
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
op.inspect("out", s)
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

### Logic Builders / Parameterized Logic

Sometimes you want to re-use a logic function in multiple steps in a
dataflow, but with slight changes and not have to repeat yourself.
Since logic functions are just functions, Python has a few techniques
to create functions that change by specific parameters dynamically:

- Create a builder function that takes the parameter and returns a
  logic function that closes over the parameter

- Use
  [`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial)

Let's demonstrate these two techniques. Let's say we want extract a
specific field from a nested structure in a stream of messages.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {
                "user_id": "1",
                "settings": {"dark_mode": True, "autosave": False, "admin": False},
            },
            {
                "user_id": "3",
                "settings": {"dark_mode": False, "autosave": False, "admin": True},
            },
            {
                "user_id": "2",
                "settings": {"dark_mode": True, "autosave": True, "admin": False},
            },
        ]
    ),
)
_ = op.inspect("check_inp", inp)
dark_modes = op.map(
    "pick_dark_mode", inp, lambda msg: (msg["user_id"], msg["settings"]["dark_mode"])
)
_ = op.inspect("check_dark_mode", dark_modes)
autosaves = op.map(
    "pick_autosave", inp, lambda msg: (msg["user_id"], msg["settings"]["autosave"])
)
_ = op.inspect("check_autosave", autosaves)
admins = op.map(
    "pick_admin", inp, lambda msg: (msg["user_id"], msg["settings"]["admin"])
)
_ = op.inspect("check_admin", admins)
run_main(flow)
```

```{testoutput}
param_eg.check_inp: {'user_id': '1', 'settings': {'dark_mode': True, 'autosave': False, 'admin': False}}
param_eg.check_dark_mode: ('1', True)
param_eg.check_autosave: ('1', False)
param_eg.check_admin: ('1', False)
param_eg.check_inp: {'user_id': '3', 'settings': {'dark_mode': False, 'autosave': False, 'admin': True}}
param_eg.check_dark_mode: ('3', False)
param_eg.check_autosave: ('3', False)
param_eg.check_admin: ('3', True)
param_eg.check_inp: {'user_id': '2', 'settings': {'dark_mode': True, 'autosave': True, 'admin': False}}
param_eg.check_dark_mode: ('2', True)
param_eg.check_autosave: ('2', True)
param_eg.check_admin: ('2', False)
```

You can see how we've plucked out just the nested field we want into
each stream. We can refactor out the duplication in the picking
functions into a builder function. The following dataflow is
equivalent.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {
                "user_id": "1",
                "settings": {"dark_mode": True, "autosave": False, "admin": False},
            },
            {
                "user_id": "3",
                "settings": {"dark_mode": False, "autosave": False, "admin": True},
            },
            {
                "user_id": "2",
                "settings": {"dark_mode": True, "autosave": True, "admin": False},
            },
        ]
    ),
)
_ = op.inspect("check_inp", inp)


def key_pick_setting(field: str):
    def picker(msg):
        return (msg["user_id"], msg["settings"][field])

    return picker


dark_modes = op.map("pick_dark_mode", inp, key_pick_setting("dark_mode"))
op.inspect("check_dark_mode", dark_modes)
autosaves = op.map("pick_autosave", inp, key_pick_setting("autosave"))
op.inspect("check_autosave", autosaves)
admins = op.map("pick_admin", inp, key_pick_setting("admin"))
op.inspect("check_admin", admins)
run_main(flow)
```

```{testoutput}
param_eg.check_inp: {'user_id': '1', 'settings': {'dark_mode': True, 'autosave': False, 'admin': False}}
param_eg.check_dark_mode: ('1', True)
param_eg.check_autosave: ('1', False)
param_eg.check_admin: ('1', False)
param_eg.check_inp: {'user_id': '3', 'settings': {'dark_mode': False, 'autosave': False, 'admin': True}}
param_eg.check_dark_mode: ('3', False)
param_eg.check_autosave: ('3', False)
param_eg.check_admin: ('3', True)
param_eg.check_inp: {'user_id': '2', 'settings': {'dark_mode': True, 'autosave': True, 'admin': False}}
param_eg.check_dark_mode: ('2', True)
param_eg.check_autosave: ('2', True)
param_eg.check_admin: ('2', False)
```

Now `key_pick_setting` builds a function each time you call it that
plucks out the requested field.

We can also use
[`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial)
to achieve the same kind of behavior.
[`functools.partial`](https://docs.python.org/3/library/functools.html#functools.partial)
lets you "pre-set" some arguments on a function and it gives you back
a function that you can still call to fill in the rest of the
arguments. The following dataflow is equivalent.

```python
import functools

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("param_eg")
inp = op.input(
    "inp",
    flow,
    TestingSource(
        [
            {
                "user_id": "1",
                "settings": {"dark_mode": True, "autosave": False, "admin": False},
            },
            {
                "user_id": "3",
                "settings": {"dark_mode": False, "autosave": False, "admin": True},
            },
            {
                "user_id": "2",
                "settings": {"dark_mode": True, "autosave": True, "admin": False},
            },
        ]
    ),
)
_ = op.inspect("check_inp", inp)


def key_pick_setting(field, msg):
    return (msg["user_id"], msg["settings"][field])


dark_modes = op.map(
    "pick_dark_mode", inp, functools.partial(key_pick_setting, "dark_mode")
)
op.inspect("check_dark_mode", dark_modes)
autosaves = op.map(
    "pick_autosave", inp, functools.partial(key_pick_setting, "autosave")
)
op.inspect("check_autosave", autosaves)
admins = op.map("pick_admin", inp, functools.partial(key_pick_setting, "admin"))
op.inspect("check_admin", admins)
run_main(flow)
```

```{testoutput}
param_eg.check_inp: {'user_id': '1', 'settings': {'dark_mode': True, 'autosave': False, 'admin': False}}
param_eg.check_dark_mode: ('1', True)
param_eg.check_autosave: ('1', False)
param_eg.check_admin: ('1', False)
param_eg.check_inp: {'user_id': '3', 'settings': {'dark_mode': False, 'autosave': False, 'admin': True}}
param_eg.check_dark_mode: ('3', False)
param_eg.check_autosave: ('3', False)
param_eg.check_admin: ('3', True)
param_eg.check_inp: {'user_id': '2', 'settings': {'dark_mode': True, 'autosave': True, 'admin': False}}
param_eg.check_dark_mode: ('2', True)
param_eg.check_autosave: ('2', True)
param_eg.check_admin: ('2', False)
```
