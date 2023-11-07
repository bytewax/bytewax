"""Time-based windowing operators.

Bytewax provides some operators and pre-built configurations for
easily grouping data into buckets called **windows** and running code
on just the values in those windows.

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
`bytewax.dataflow.Dataflow`.

4. Pass both these configs to the windowing operator of your
choice. The **windowing operators** decide what kind of logic you
should apply to values within a window and what should be the output
of the window. E.g. `bytewax.dataflow.Dataflow.reduce_window` combines
all values in a window into a single output and sends that downstream.

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
`bytewax.run` for more info), so failure in the middle of a window
will be handled as gracefully as possible.

Some clocks don't have a single correct answer on what to do during
recovery. E.g. if you use `SystemClockConfig` with 10 minute windows,
but then recover on a 15 minute mark, the system will immediately
close out the half-completed window stored during recovery. See the
docs for each `ClockConfig` subclass for specific notes on recovery.

Recovery happens on the granularity of the _epochs_ of the dataflow,
not the windows. Epoch interval has no affect on windowing operator
behavior when there are no failures; it is solely an implementation
detail of the recovery system. See `bytewax.run` for more information
on epochs.

"""

from functools import partial
from typing import (
    Any,
    Callable,
    Type,
)

from bytewax.dataflow import (
    Dataflow,
    KeyedStream,
    Stream,
    operator,
)
from bytewax.operators import _identity, _JoinState, _none_builder

from ..bytewax import (  # noqa: F401
    ClockConfig,
    EventClockConfig,
    SessionWindow,
    SlidingWindow,
    SystemClockConfig,
    TumblingWindow,
    WindowConfig,
    WindowMetadata,
)

__all__ = [
    "ClockConfig",
    "EventClockConfig",
    "SessionWindow",
    "SlidingWindow",
    "SystemClockConfig",
    "TumblingWindow",
    "WindowConfig",
    "WindowMetadata",
    "collect_window",
    "count_window",
    "fold_window",
    "join_window",
    "join_window_named",
    "max_window",
    "min_window",
    "reduce_window",
]


def _list_collector(s, v):
    s.append(v)
    return s


def _set_collector(s, v):
    s.add(v)
    return s


def _dict_collector(s, k_v):
    k, v = k_v
    s[k] = v
    return s


def _get_collector(t: Type) -> Callable:
    if issubclass(t, list):
        collector = _list_collector
    elif issubclass(t, set):
        collector = _set_collector
    elif issubclass(t, dict):
        collector = _dict_collector
    else:
        msg = (
            f"collect doesn't support `{t:!}`; "
            "only `list`, `set`, and `dict`; use `fold` operator directly"
        )
        raise TypeError(msg)
    return collector


@operator()
def collect_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type = list,
) -> KeyedStream:
    """Collect all items in a window into a container.

    Args:
        up: Stream of items to count.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        into: Type to collect into. Defaults to `list`.

    Returns:
        A keyed stream of the collected containers at the end of each
        window.

    """
    collector = _get_collector(into)

    return up.fold_window("fold_window", clock, windower, into, collector)


@operator()
def count_window(
    up: Stream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    key: Callable[[Any], str],
) -> KeyedStream:
    """Count the number of occurrences of items in a window.

    Args:
        up: Stream of items to count.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Returns:
        A stream of `(key, count)` per window at the end of each window.

    """
    return (
        up.map("init_count", lambda x: (key(x), 1))
        .key_assert("keyed")
        .reduce_window("sum", clock, windower, lambda s, x: s + x)
    )


@operator(_core=True)
def fold_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
) -> KeyedStream:
    """Build an empty accumulator, then combine values into it.

    It is like `reduce_window` but uses a function to build the initial
    value.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        builder: Called the first time a key appears and is expected
            to return the empty accumulator for that key.

        folder: Combines a new value into an existing accumulator and
            returns the updated accumulator. The accumulator is
            initially the empty accumulator.

    Returns:
        A keyed stream of the accumulators once each window has
        closed.

    """
    return KeyedStream(f"{up._scope.parent_id}.down", up._scope)


def _join_window_folder(state: _JoinState, name_value) -> _JoinState:
    name, value = name_value
    state.add_val(name, value)
    return state


@operator()
def join_window(
    left: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    *rights: KeyedStream,
) -> KeyedStream:
    """Gather together the value for a key on multiple streams.

    Args:
        left: Keyed stream.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        *rights: Other keyed streams.

    Returns:
        Emits a tuple with the value from each stream in the order of
        the argument list once each window has closed.

    """
    named_ups = dict((str(i), s) for i, s in enumerate([left] + list(rights)))
    names = list(named_ups.keys())

    return (
        left.flow()
        ._join_name_merge("add_names", **named_ups)
        .fold_window(
            "join",
            clock,
            windower,
            lambda: _JoinState.for_names(names),
            _join_window_folder,
        )
        .flat_map_value("astuple", _JoinState.astuples)
    )


@operator()
def join_window_named(
    flow: Dataflow,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    **ups: KeyedStream,
) -> KeyedStream:
    """Gather together the value for a key on multiple named streams.

    Args:
        flow: Dataflow.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        **ups: Named keyed streams. The name of each stream will be
            used in the emitted `dict`s.

    Returns:
        Emits a `dict` mapping the name to the value from each stream
        once each window has closed.

    """
    names = list(ups.keys())

    return (
        flow._join_name_merge("add_names", **ups)
        .fold_window(
            "join",
            clock,
            windower,
            lambda: _JoinState.for_names(names),
            _join_window_folder,
        )
        .flat_map_value("asdict", _JoinState.asdicts)
    )


@operator()
def max_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    by: Callable[[Any], Any] = _identity,
) -> KeyedStream:
    """Find the minumum value for each key.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the min values once each window has closed.

    """
    return up.reduce_window("reduce_window", clock, windower, partial(max, key=by))


@operator()
def min_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    by: Callable[[Any], Any] = _identity,
) -> KeyedStream:
    """Find the minumum value for each key.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the min values once each window has closed.

    """
    return up.reduce_window("reduce_window", clock, windower, partial(min, key=by))


@operator()
def reduce_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    reducer: Callable[[Any, Any], Any],
) -> KeyedStream:
    """Distill all values for a key down into a single value.

    It is like `fold_window` but the first value is the initial
    accumulator.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        reducer: Combines a new value into an old value and returns
            the combined value.

    Returns:
        A keyed stream of the reduced values once each window has
        closed.

    """

    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold_window("fold_window", clock, windower, _none_builder, shim_folder)
