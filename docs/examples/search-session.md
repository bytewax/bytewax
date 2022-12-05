Building Sessions from Search Logs
==================================

Here is a basic example of using Bytewax to turn an incoming stream of
event logs from a hypothetical search engine into metrics over search
sessions. In this example, we're going to focus on the dataflow itself
and aggregating state, and gloss over details of building this
processing into a larger system.

Schema
------

Let's start by defining a data model / schema for our incoming
events. We'll make a little model class for all the relevant events
we'd want to monitor.

```python
from dataclasses import dataclass
from typing import List


@dataclass
class AppOpen:
    user: int


@dataclass
class Search:
    user: int
    query: str


@dataclass
class Results:
    user: int
    items: List[str]


@dataclass
class ClickResult:
    user: int
    item: str


@dataclass
class AppClose:
    user: int


@dataclass
class Timeout:
    user: int
```

In a more mature system, these might come from external schema or be
auto generated.

Epochs
------

Now that we've got those, here's a small dump of some example data you
could imagine coming from your app's events infrastructure.

Let's write a function that will yield one of these events at a time
into our dataflow.

```python
IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS = [
    AppOpen(user=1),
    Search(user=1, query="dogs"),
    # Eliding named args...
    Results(1, ["fido", "rover", "buddy"]),
    ClickResult(1, "rover"),
    Search(1, "cats"),
    Results(1, ["fluffy", "burrito", "kathy"]),
    ClickResult(1, "fluffy"),
    AppOpen(2),
    ClickResult(1, "kathy"),
    Search(2, "fruit"),
    AppClose(1),
    AppClose(2),
]


def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    for line in IMAGINE_THESE_EVENTS_STREAM_FROM_CLIENTS:
        yield (state, line)
```

For the moment, we aren't going to be using our resume state to manage failures,
but returning the empty state is a requirement for our input builder.

High-Level Plan
---------------

Let's talk about the tasks high-level plan for how to sessionize:

- Searches are per-user, so we need to divvy up events by user.

- Searches don't span user sessions, so we should calculate user
  sessions first.

- Sessions without a search shouldn't contribute.

- Calculate one metric: **click through rate** (or **CTR**), if a user
  clicked on any result in a search.

The Dataflow
------------

Now that we have some input data, let's start defining the
computational steps of our dataflow based on our plan.

To start, create an empty `bytewax.dataflow.Dataflow` object.

In this case, we'll use a `ManualInputConfig`, which takes the
`input_builder` function that we defined above.

```python
from bytewax.inputs import ManualInputConfig
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
```

Now that we have a Dataflow, and some input, we can add a series of **steps**
to the dataflow. Steps are made up of **operators**, that provide a "shape"
of transformation, and **logic functions**, that you supply to do your specific
transformation. [You can read more about all the operators in our
documentation.](/docs/getting-started/operators/)

Our first task is to make sure to group incoming events by user since
no session deals with multiple users.

All Bytewax operators that perform grouping require that their input
be in the form of a `(key, value)` tuple, where `key` is the string
the dataflow will group by before passing to the operator logic.

The operator which modifies all data flowing through it is
[map](/apidocs#bytewax.Dataflow.map). Let's use that and pull each
event's user ID as a string into that key position.

```python
def initial_session(event):
    return str(event.user), [event]


flow.map(initial_session)
```

For the value, we're planning ahead a little bit to our next task:
sessionization. The operator best shaped for this is the [reduce
operator](/apidocs#bytewax.Dataflow.reduce) which groups items by key, then combines
them together into an **aggregator** in order. We can think about our
reduce step as "combine together sessions if they should be
joined". We'll be modeling a session as a list of events, so have the
values be a list of a single event `[event]` that we will combine with
our reducer function.

Reduce requires two bits of logic:

- How do I combine sessions? Since session are just Python lists, we
  can use the `+` operator to add them (via the built-in
  `operator.add` function).

- When is a session complete? In this case, a session is complete when
  the last item in the session is the app closing. We'll write a
  `session_has_closed` function to answer that.

Reduce also takes a unique **step ID** to help organize the state
saved internally.

```python
import operator


def session_has_closed(session):
    # isinstance does not work on objects sent through pickling, which
    # Bytewax does when there are multiple workers.
    return type(session[-1]).__name__ == "AppClose"


flow.reduce("sessionizer", operator.add, session_has_closed)
```

We had to group by user because sessions were per-user, but now that
we have sessions, the grouping key is no longer necessary for
metrics. Let's remove it with another map.

```python
def remove_key(user_event):
    user, event = user_event
    return event


flow.map(remove_key)
```

Our next task is to split user sessions into search sessions. To do
that, we'll use the [flat map operator](/apidocs#bytewax.Dataflow.flat_map), that
allows you to emit multiple items downstream (search sessions) for
each input item (user session).

We walk through each user session's events, then whenever we encounter
a search, emit downstream the previous events. This works just like
`str.split` but with objects.

```python
def is_search(event):
    return type(event).__name__ == "Search"


def split_into_searches(user_session):
    search_session = []
    for event in user_session:
        if is_search(event):
            yield search_session
            search_session = []
        search_session.append(event)
    yield search_session


flow.flat_map(split_into_searches)
```

We can use the [filter operator](/apidocs#bytewax.Dataflow.filter) to get rid of all
search sessions that don't contain searches and shouldn't contribute
to metrics.

```python
def has_search(search_session):
    return any(is_search(event) for event in search_session)


flow.filter(has_search)
```

We can now move on to our final task: generating metric observations
per search session. If there's a click during a search, the CTR is 1.0
for that search, 0.0 otherwise. Given those two extreme values, we can
do further statistics to get things like CTR per day, etc

```python
def has_click(search_session):
    return any(type(event).__name__ == "ClickResult" for event in search_session)


def calc_ctr(search_session):
    if has_click(search_session):
        return 1.0
    else:
        return 0.0


flow.map(calc_ctr)
```

Now that our dataflow is done, we can define a function to be called for each output item.
In this example, we're just printing out what we've received.


```python
from bytewax.outputs import ManualOutputConfig


def output_builder(worker_index, worker_count):
    return print


flow.capture(ManualOutputConfig(output_builder))
```

Now we're done with defining the dataflow. Let's run it!

Execution
---------

[Bytewax provides a few different entry points for executing your
dataflow](/docs/getting-started/execution/), but because we're focusing on the dataflow in
this example, we're going to use `bytewax.execution.run_main` which is the most basic
execution mode.

Let's take our example list of events and pipe it in:

```python
from bytewax.execution import run_main

run_main(flow)
```

Let's inspect the output and see if it makes sense.

```{testoutput}
1.0
1.0
0.0
```

Since the [capture](/apidocs#bytewax.Dataflow.capture) step is immediately after
calculating CTR, we should see one output item for each search
session. That checks out! There were three searches in the input:
"dogs", "cats", and "fruit". Only the first two resulted in a click,
so they contributed `1.0` to the CTR, while the no-click search
contributed `0.0`.

