# Collecting and Windowing Example

In this section, we'll be talking about the collect and window
operators that Bytewax provides.

## Collecting

The {py:obj}`~bytewax.operators.collect` operator operates over a
stream of data and collects a number of items until a max size, or a
given timeout is reached.

Let's construct a simple dataflow to demonstrate. In the following
dataflow, we're using the {py:obj}`~bytewax.testing.TestingSource` to
generate sample data from a list. In a production dataflow, your input
could come from [Redpanda](https://redpanda.com/), or any other input
source.

The {py:obj}`~bytewax.testing.TestingSource` emits one integer at a
time into the dataflow. In our {py:obj}`~bytewax.operators.collect`
operator, we'll configure it to wait for either 3 values, or for 10
seconds to expire before emitting the list downstream. Since we won't
be waiting for data with our {py:obj}`~bytewax.testing.TestingSource`,
we should see list of 3 items until we run out of input from our
{py:obj}`~bytewax.testing.TestingSource`.

It is important to remember that the
{py:obj}`~bytewax.operators.collect` operator collects data based on a
key. We'll use the {py:obj}`~bytewax.operators.key_on` operator to
give all of our items the same fixed key so they are processed
together.

Copy the following code into a file named `collect_example.py`:

```python
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import TestingSource

flow = Dataflow("collect")
stream = op.input("input", flow, TestingSource(list(range(10))))
# We want to consider all the items together, so we assign the same fixed key to each of them.
keyed_stream = op.key_on("key", stream, lambda _x: "ALL")
collected_stream = op.collect(
    "collect", keyed_stream, timeout=timedelta(seconds=10), max_size=3
)
op.output("out", collected_stream, StdOutSink())
```

Now we have our dataflow, we can run our example:

```shell
> python -m bytewax.run collect_example
```

```{testcode}
:hide:

bytewax.testing.run_main(flow)
```

You should see output:

```{testoutput}
('ALL', [0, 1, 2])
('ALL', [3, 4, 5])
('ALL', [6, 7, 8])
('ALL', [9])
```

## Windowing

Windowing operators (which live in {py:obj}`bytewax.operators.window`)
perform computation over a time-based window of data where time can be
defined as the system time that the data is processed, known as
**processing time**, or time as a property of the data itself referred
to as **event time**. For this example, we're going to use event time.
Time will be used in our window operators to decide which windows a
given item belongs to, and to determine when an item is late.

Let's start by and importing the relevant classes, creating a
dataflow, and configuring some test input.

```python
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.window as win

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.operators.window import EventClock, TumblingWindower, WindowMetadata
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

### Windowers and Clocks

In addition to a sense of time, windowing operators also require a
configuration that determines how items are assigned to windows. Items
can be assigned to one or more windows, depending on the desired
behavior. In this example, we'll be using the
{py:obj}`~bytewax.operators.window.TumblingWindower` assigner, which
will assign each item to a single, fixed duration window for each key
in the stream.

In the following snippet, we configure the
{py:obj}`~bytewax.operators.window.EventClock` to determine the
time of items flowing through the dataflow with a
<inv:python:std:term#lambda> that reads the "time" key of each item we
created in the dictionary above.

We also configure our {py:obj}`~bytewax.operators.window.EventClock`
with a value for the
{py:obj}`~bytewax.operators.window.EventClock.wait_for_system_duration`
parameter.

{py:obj}`~bytewax.operators.window.EventClock.wait_for_system_duration`
is the amount of system time we're willing to wait for any late
arriving items before closing the window and emitting it downstream.
After a window is closed, late arriving items for that window will be
discarded.

In order for windows to be generated consistently, we finally supply
the {py:obj}`~bytewax.operators.window.TumblingWindower.align_to`
parameter, which says that all windows that we collect will be aligned
to the given `datetime`. We'll use the value that we created above for
our event data.

```python
clock = EventClock(lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0))
windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)
```

### Window Operators

Now that we have our windower and clock, we need to define the
processing step or operation that we would like to perform on each
window. We'll use the
{py:obj}`~bytewax.operators.window.collect_window` operator to collect
all of the events for a given user in each window.

```python
win_out = win.collect_window("add", keyed_stream, clock, windower)
```

Every windowing operator results in three distinct downstreams
packaged into a {py:obj}`~bytewax.operators.window.WindowOut` object:

1. The result stream, named `down`.

2. Late items.

3. Window metadata.

In this introduction, we'll focus just on the result stream.

### Result Stream

The result stream of all window operators is a tuple in the format:
`(key, (window_id, value))` where `window_id` is an opaque ID for the
window that resulted in this value. In this case, since we used the
{py:obj}`~bytewax.operators.window.collect_window` operator, the
downstream values will be the collected list of all items in the
window.

We'll write this to standard output using our output operator:

```python
op.output("out", win_out.down, StdOutSink())
```

### Running our Dataflow

We can run our dataflow with:

```shell
python -m bytewax.run window_example
```

Running our dataflow, we should see the following output:

```{testcode}
:hide:

import bytewax.testing

bytewax.testing.run_main(flow)
```

```{testoutput}
('a', (0, [{'time': datetime.datetime(2022, 1, 1, 0, 0, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 1}, {'time': datetime.datetime(2022, 1, 1, 0, 0, 4, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 1}, {'time': datetime.datetime(2022, 1, 1, 0, 0, 8, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 1}]))
('b', (0, [{'time': datetime.datetime(2022, 1, 1, 0, 0, 5, tzinfo=datetime.timezone.utc), 'user': 'b', 'val': 1}]))('a', (1, [{'time': datetime.datetime(2022, 1, 1, 0, 0, 12, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 1}, {'time': datetime.datetime(2022, 1, 1, 0, 0, 13, tzinfo=datetime.timezone.utc), 'user': 'a', 'val': 1}]))
('b', (1, [{'time': datetime.datetime(2022, 1, 1, 0, 0, 14, tzinfo=datetime.timezone.utc), 'user': 'b', 'val': 1}]))
```

Bytewax has created a window for each key (in our case, the user
value) and collected all of the items that were encountered in that
window.

## Wrapping Up

Bytewax offers multiple processing shapes, window assignment types and
other configuration options. For more detailed information about
windowing, please see the <project:#xref-windowing> section, and the
windowing API documentation in {py:obj}`bytewax.operators.window`.
