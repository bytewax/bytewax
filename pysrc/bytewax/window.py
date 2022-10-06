"""Time-based windows.

Bytewax provides some operators and pre-built configurations for
easily grouping data into buckets called **windows** and running code
on just the values in those windows.

See the operator methods on `bytewax.dataflow.Dataflow` with `_window`
in the name for simple example use cases of each.

Use
---

1. Pick a clock and create a config for it. A **clock** determines the
time of each element and the current time used for closing each
window. E.g. use the current system time. See the docs for each
subclass of `ClockConfig` for options.

2. Pick a windower and create a config for it. A **windower** defines
how to take the values and their times and bucket them into
windows. E.g. have tumbling windows every 30 seconds. See the docs for
each subclass of `WindowConfig` for options.

3. Pick a **key** to route the values for the window and make sure the
input to the windowing operator you choose is a 2-tuple of `(key: str,
value)`. Windows are managed independently for each key. If you need
all data to be processed into the same window state, you can use a
constant key like `("ALL", value)` but this will reduce the
parallelism possible in the dataflow. This is similar to all the other
stateful operators, so you can read more on their methods on
`bytewax.Dataflow`.

4. Pass both these configs to the windowing operator of your
choice. The **windowing operators** decide what kind of logic you
should apply to values within a window and what should be the output
of the window. E.g. `bytewax.Dataflow.reduce_window` combines all
values in a window into a single output and sends that downstream.

You are allowed and encouraged to have as many different clocks and
windowers as you need in a single dataflow. Just instantiate more of
them and pass the ones you need for each situation to each windowing
operator.

Order
-----

Because Bytewax can be run as a distributed system with multiple
worker processes and threads all reading relevant data simultaneously,
you have to specifically collect and manually sort data that you need
to process in strict time order.

Recovery
--------

Bytewax's windowing system is built on top of its recovery system (see
`bytewax.recovery` for more info), so failure in the middle of a
window will be handled as gracefully as possible.

Some clocks don't have a single correct answer on what to do during
recovery. E.g. if you use `SystemClockConfig` with 10 minute windows,
but then recover on a 15 minute mark, the system will immediately
close out the half-completed window stored during recovery. See the
docs for each `ClockConfig` subclass for specific notes on recovery.

Recovery happens on the granularity of the _epochs_ of the dataflow,
not the windows. See `bytewax.inputs` for more information on ways to
adjust epochs.

"""

from .bytewax import (  # noqa: F401
    ClockConfig,
    EventClockConfig,
    SystemClockConfig,
    TumblingWindowConfig,
    WindowConfig,
)
