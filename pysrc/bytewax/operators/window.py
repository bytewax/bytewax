"""Time-based windowing operators."""

from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Set,
    Tuple,
    Type,
    TypeVar,
    overload,
)

import bytewax.operators as op
from bytewax.dataflow import (
    Stream,
    operator,
)
from bytewax.operators import KeyedStream, _identity, _JoinState, _untyped_none

from ..bytewax import (  # type: ignore[import]
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

C = TypeVar("C", bound=Iterable)
K = TypeVar("K")
X = TypeVar("X")  # Item
Y = TypeVar("Y")  # Output Item
V = TypeVar("V")  # Value
W = TypeVar("W")  # Output Value
S = TypeVar("S")  # State


def _list_collector(s: List[V], v: V) -> List[V]:
    s.append(v)
    return s


def _set_collector(s: Set[V], v: V) -> Set[V]:
    s.add(v)
    return s


def _dict_collector(s: Dict[K, V], k_v: Tuple[K, V]) -> Dict[K, V]:
    k, v = k_v
    s[k] = v
    return s


def _get_collector(t: Type) -> Callable:
    if issubclass(t, list):
        return _list_collector
    elif issubclass(t, set):
        return _set_collector
    elif issubclass(t, dict):
        return _dict_collector
    else:
        msg = (
            f"collect doesn't support `{t:!}`; "
            "only `list`, `set`, and `dict`; use `fold` operator directly"
        )
        raise TypeError(msg)


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
) -> KeyedStream[List[V]]:
    ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type[List],
) -> KeyedStream[List[V]]:
    ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type[Set],
) -> KeyedStream[Set[V]]:
    ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[Tuple[K, V]],
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type[Dict],
) -> KeyedStream[Dict[K, V]]:
    ...


@operator
def collect_window(
    step_id: str,
    up: KeyedStream,
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type = list,
) -> KeyedStream:
    """Collect items in a window into a container.

    Args:
        step_id: Unique ID.

        up: Stream of items to count.

        clock: Clock.

        windower: Windower.

        into: Type to collect into. Defaults to `list`.

    Returns:
        A keyed stream of the collected containers at the end of each
        window.

    """
    collector = _get_collector(into)

    return fold_window("fold_window", up, clock, windower, lambda: into(), collector)


@operator
def count_window(
    step_id: str,
    up: Stream[X],
    clock: ClockConfig,
    windower: WindowConfig,
    key: Callable[[X], str],
) -> KeyedStream[int]:
    """Count the number of occurrences of items in a window.

    Args:
        step_id: Unique ID.

        up: Stream of items to count.

        clock: Clock.

        windower: Windower.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Returns:
        A stream of `(key, count)` per window at the end of each window.

    """
    init_count: KeyedStream[int] = op.map("init_count", up, lambda x: (key(x), 1))
    return reduce_window("sum", init_count, clock, windower, lambda s, x: s + x)


@operator(_core=True)
def fold_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    builder: Callable[[], S],
    folder: Callable[[S, V], S],
) -> KeyedStream[S]:
    """Build an empty accumulator, then combine values into it.

    It is like `reduce_window` but uses a function to build the initial
    value.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

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
    return Stream(f"{up._scope.parent_id}.down", up._scope)


def _join_window_folder(state: _JoinState, name_value: Tuple[str, Any]) -> _JoinState:
    name, value = name_value
    state.add_val(name, value)
    return state


@operator
def join_window(
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    *sides: KeyedStream[Any],
) -> KeyedStream[Tuple]:
    """Gather together the value for a key on multiple streams.

    Args:
        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        *sides: Keyed streams.

    Returns:
        Emits a tuple with the value from each stream in the order of
        the argument list once each window has closed.

    """
    named_sides = dict((str(i), s) for i, s in enumerate(sides))
    names = list(named_sides.keys())

    merged = op._join_name_merge("add_names", **named_sides)

    def builder() -> _JoinState:
        return _JoinState.for_names(names)

    joined = fold_window(
        "join",
        merged,
        clock,
        windower,
        builder,
        _join_window_folder,
    )
    return op.flat_map_value("astuple", joined, _JoinState.astuples)


@operator
def join_window_named(
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    **sides: KeyedStream[Any],
) -> KeyedStream[Dict[str, Any]]:
    """Gather together the value for a key on multiple named streams.

    Args:
        step_id: Unique ID.

        clock: Clock.

        windower: Windower.

        **sides: Named keyed streams. The name of each stream will be
            used in the emitted `dict`s.

    Returns:
        Emits a `dict` mapping the name to the value from each stream
        once each window has closed.

    """
    names = list(sides.keys())

    merged = op._join_name_merge("add_names", **sides)

    def builder() -> _JoinState:
        return _JoinState.for_names(names)

    joined = fold_window(
        "join",
        merged,
        clock,
        windower,
        builder,
        _join_window_folder,
    )
    return op.flat_map_value("asdict", joined, _JoinState.asdicts)


@overload
def max_window(
    step_id: str, up: KeyedStream[V], clock: ClockConfig, windower: WindowConfig
) -> KeyedStream[V]:
    ...


@overload
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    by: Callable[[V], Any],
) -> KeyedStream[V]:
    ...


@operator
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    by=_identity,
) -> KeyedStream:
    """Find the minumum value for each key.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        clock: Clock.

        windower: Windower.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the min values once each window has closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(max, key=by))


@overload
def min_window(
    step_id: str, up: KeyedStream[V], clock: ClockConfig, windower: WindowConfig
) -> KeyedStream[V]:
    ...


@overload
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    by: Callable[[V], Any],
) -> KeyedStream[V]:
    ...


@operator
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    by=_identity,
) -> KeyedStream:
    """Find the minumum value for each key.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        clock: Clock.

        windower: Windower.

        by: A function called on each value that is used to extract
            what to compare.

    Returns:
        A keyed stream of the min values once each window has closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(min, key=by))


@operator
def reduce_window(
    step_id: str,
    up: KeyedStream[V],
    clock: ClockConfig,
    windower: WindowConfig,
    reducer: Callable[[V, V], V],
) -> KeyedStream[V]:
    """Distill all values for a key down into a single value.

    It is like `fold_window` but the first value is the initial
    accumulator.

    Args:
        step_id: Unique ID.

        up: Keyed stream.

        clock: Clock.

        windower: Windower.

        reducer: Combines a new value into an old value and returns
            the combined value.

    Returns:
        A keyed stream of the reduced values once each window has
        closed.

    """

    def shim_folder(s: V, v: V) -> V:
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return fold_window("fold_window", up, clock, windower, _untyped_none, shim_folder)
