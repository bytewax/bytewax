In this section, we'll be talking about the Batching and Windowing operators
that Bytewax provides.

In this context, **Batching** refers to an operation over an unbounded stream
of data, rather than an execution mode where the amount of data is finite.

It is important to remember that all **Batching** and **Windowing** operators
collect data based on a key and therefore expect input `Streams` to be keyed.


## Batching

**Batching** operators operate over a stream of data and collect a number of
items until a fixed size, or a given timeout is reached.

Let's construct a simple dataflow to demonstrate. In the following Dataflow,
we're using the `TestingSource` **Source** to generate data from a list,
but in a production Dataflow, your input could come from RedPanda, or another
other streaming input source.

The `TestingSource` emits one integer at a time into the dataflow. In our
batch operator, we'll configure it to wait for either three values, or
for 10 seconds to expire before emitting the batch downstream. Since
we won't be waiting for data with our `TestingSource`, we should
always see batches of 3 items.

```python
from datetime import timedelta

import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import TestingSource

flow = Dataflow("batching")
stream = op.input("input", flow, TestingSource(list(range(10))))
# Here we want to batch all the items together, so we use the same fixed key for all the items
keyed_stream = op.key_on("key", stream, lambda _x: "ALL")
batched_stream = op.batch(
    "batch", keyed_stream, timeout=timedelta(seconds=10), batch_size=3
)
op.output("out", batched_stream, StdOutSink())
```

Great, now we can run our batch example. You should see the following output:

```
('ALL', [0, 1, 2])
('ALL', [3, 4, 5])
('ALL', [6, 7, 8])
('ALL', [9])
```

## Windowing

Windowing operators perform computation over a time based window of data
where time can be defined as the system time that the data is processed, known
as **Processing Time**, or time as a property of the data itself referred to
as **Event Time**. For this example, we're going to use **Event Time**. Time
will be used in our window operators to decide which windows a given item belongs
to, and to determine when an item is late.

Let's start by and importing the relevant classes, creating a Dataflow, and
configuring some test input.

```python
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import EventClockConfig, TumblingWindow, WindowMetadata
from bytewax.testing import TestingSource

flow = Dataflow("windowing")

align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
inp = [
    {"time": align_to, "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=5), "user": "b", "val": 1},
    {"time": align_to + timedelta(seconds=8), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
    {"time": align_to + timedelta(seconds=14), "user": "b", "val": 1},
]
stream = op.input("input", flow, TestingSource(inp))
keyed_stream = op.key_on("key_on_user", stream, lambda e: e["user"])
```

## Window assignment, and late arriving data

In addition to a sense of time, windowing operators also require a configuration
that determines how items are assigned to windows. Items can be assigned to
one or more windows, depending on the desired behavior. In this example, we'll
be using the `TumblingWindow` assigner, which will assign each item to a single,
fixed duration window for each key in the stream.

In the following snippet, we configure the `EventClockConfig` to determine
the time of items flowing through the dataflow with a `lambda` that reads
the "time" key of each item we created in the dictionary above.

We also configure our `EventClockConfig` with a `wait_for_system_duration`,
which is the amount of system time we're willing to wait for any late arriving
items before closing the window and emitting it downstream. After a window
is closed, late arriving items for that window will be discarded.

In order for windows to be generated consistently, we supply the `align_to`
parameter, which says that all windows that we collect will be aligned to
this `datetime`. We'll use the value that we created above.

```python
ZERO_TD = timedelta(seconds=0)
clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
windower = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)
```

## Window processing

Now that we have our window assignment, and our clock configuration, we need
to define the processing step that we would like to perform on each window.
We'll use the `reduce_window` operator to keep a windowed count of the values
for our user in each window.

```python
def add(acc, x):
    # `acc` here is the accumulated value, which is the first item
    # encountered for a key for this window.
    acc["val"] += x["val"]
    # We'll update the first value we encountered with the value
    # from our new item `x`
    return acc


windowed_stream = win.reduce_window("add", keyed_stream, clock, windower, add)
```

## Window Metadata

The output of window operators in Bytewax is a tuple in the format: `(key, (metadata, window))`
Where `metadata` is a `WindowMetadata` object with information about the `open_time` and `close_time`
of the window.

We'll write our window output to STDOUT using our output operator:

```python
op.output("out", windowed_stream, StdOutSink())
```

## Running the dataflow

Running our dataflow, we should see the following output:

```
('a', (WindowMetadata(open_time: 2022-01-01 00:00:00 UTC, close_time: 2022-01-01 00:00:10 UTC), {'time': datetime.datetime(2022, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 3}))
('b', (WindowMetadata(open_time: 2022-01-01 00:00:00 UTC, close_time: 2022-01-01 00:00:10 UTC), {'time': datetime.datetime(2022, 1, 1, 0, 0, 5, tzinfo=datetime.timezone.utc), 'user': 'b', 'val': 1}))
('a', (WindowMetadata(open_time: 2022-01-01 00:00:10 UTC, close_time: 2022-01-01 00:00:20 UTC), {'time': datetime.datetime(2022, 1, 1, 0, 0, 12, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 2}))
('b', (WindowMetadata(open_time: 2022-01-01 00:00:10 UTC, close_time: 2022-01-01 00:00:20 UTC), {'time': datetime.datetime(2022, 1, 1, 0, 0, 14, tzinfo=datetime.timezone.utc), 'user': 'b', 'val': 1}))
```

Bytewax has created a window for each key (in our case, the user value) and kept
a running count of the `val` field by updating the first item with subsequent
values. Along with the key for each value, we receive a `WindowMetadata` object
with details about each window that Bytewax created.

## Wrapping up

Bytewax offers multiple processing shapes, window assignment types and other
configuration options. For more information, please see the [Windowing](/docs/articles/concepts/windowing.md)
section, and the [API Documentation](/apidocs/).
