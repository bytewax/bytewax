"""Built-in operators.

See the `bytewax` module docstring for the basics of building and
running dataflows.

See `bytewax.operators.window` for windowing operators.

# Stateful Operators

Operators loaded onto `KeyedStream` (e.g. `fold.fold`,
`stateful_map.stateful_map`, etc.) will only work when this input
stream is keyed, and automatically unwrap value out of the `(key,
value)` 2-tuples upstream and then automatically re-wrap any emitted
values back into `(key, value)` 2-tuples. See the operators
`key_on.key_on` and `key_assert.key_assert` to create
`KeyedStream`s.

# Non-Built-In Operators

All operators in this module are automatically loaded when you `import
bytewax`.

Operators defined elsewhere must be loaded. See
`bytewax.dataflow.load_mod_ops` and the `bytewax.dataflow` module
docstring for how to load custom operators.

See the `bytewax.dataflow` module docstring for how to define your own
custom operators.

"""

import copy
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

from bytewax.dataflow import (
    Dataflow,
    KeyedStream,
    MultiStream,
    Stream,
    f_repr,
    operator,
)
from bytewax.inputs import Source
from bytewax.outputs import Sink


def _identity(x):
    return x


def _none_builder():
    return None


def _never_complete(_):
    return False


class UnaryLogic(ABC):
    """Abstract class to define the behavior of a `unary` operator.

    The operator will call these methods in order: `on_item` once for
    any items queued, then `on_notify` if the notification time has
    passed, then `on_eof` if the upstream is EOF and no new items will
    be received this execution. If the logic is retained after all the
    above calls then `notify_at` will be called. `snapshot` is
    periodically called.

    """

    #: This logic should be retained after this call to `on_*`.
    #
    #: If you always return this, this state will never be deleted and
    #: if your key-space grows without bound, your memory usage will
    #: also grow without bound.
    RETAIN: bool = False

    #: This logic should be discarded immediately after this call to
    #: `on_*`.
    DISCARD: bool = True

    @abstractmethod
    def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
        """Called on each new upstream item.

        This will be called multiple times in a row if there are
        multiple items from upstream.

        Args:
            now: The current `datetime`.

            value: The value of the upstream `(key, value)`.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
        """Called when the scheduled notification time has passed.

        Args:
            sched: The scheduled notification time.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        """The upstream has no more items on this execution.

        This will only be called once per execution after `on_item` is
        done being called.

        Returns:
            A 2-tuple of: any values to emit downstream and wheither
            to discard this logic. Values will be wrapped in `(key,
            value)` automatically.

        """
        ...

    @abstractmethod
    def notify_at(self) -> Optional[datetime]:
        """Return the next notification time.

        This will be called once right after the logic is built, and
        if any of the `on_*` methods were called if the logic was
        retained by `is_complete`.

        This must always return the next notification time. The
        operator only stores a single next time, so if

        Returns:
            Scheduled time. If `None`, no `on_notify` callback will
            occur.

        """
        ...

    @abstractmethod
    def snapshot(self) -> Any:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of `unary.unary` when resuming.

        The state must be `pickle`-able.

        **The state must be effectively immutable!** If any of the
        other functions in this class might be able to mutate the
        state, you must `copy.deepcopy` or something equivalent before
        returning it here.

        """
        ...


@operator(_core=True)
def _noop(up: Stream, step_id: str) -> Stream:
    """No-op; is compiled out when making the Timely dataflow.

    Sometimes necessary to ensure `Dataflow` structure is valid.

    """
    raise NotImplementedError()


@dataclass
class _BatchState:
    acc: List[Any] = field(default_factory=list)
    timeout_at: Optional[datetime] = None


@dataclass
class _BatchLogic(UnaryLogic):
    step_id: str
    timeout: timedelta
    batch_size: int
    state: _BatchState

    def on_item(self, now: datetime, v: Any) -> Tuple[Iterable[Any], bool]:
        self.state.timeout_at = now + self.timeout

        self.state.acc.append(v)
        if len(self.state.acc) >= self.batch_size:
            # No need to deepcopy because we are discarding the state.
            return ([self.state.acc], UnaryLogic.DISCARD)

        return ([], UnaryLogic.RETAIN)

    def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        return self.state.timeout_at

    def snapshot(self) -> Any:
        return copy.deepcopy(self.state)


@operator()
def batch(
    up: KeyedStream, step_id: str, timeout: timedelta, batch_size: int
) -> KeyedStream:
    """Batch incoming items up to a size or a timeout.

    Args:
        up: Stream of individual items.

        step_id: Unique ID.

        timeout: Timeout before emitting the batch, even if max_size
            was not reached.

        batch_size: Maximum size of the batch.

    Returns:
        A stream of batches of upstream items gathered into a `list`.

    """

    def shim_builder(_now: datetime, resume_state: Optional[Any]) -> _BatchLogic:
        state = resume_state if resume_state is not None else _BatchState()
        return _BatchLogic(step_id, timeout, batch_size, state)

    return up.unary("unary", shim_builder)


@dataclass(frozen=True)
class BranchOut:
    """Streams returned from `branch` operator.

    You can tuple unpack this for convenience.

    >>> from bytewax.connectors.stdio import StdOutSink
    >>> from bytewax.run import run_main
    >>> flow = Dataflow("my_flow")
    >>> nums = flow.input("nums", TestingSource([1, 2, 3, 4, 5]))
    >>> evens, odds = nums.split("split_even", lambda x: x % 2 == 0)
    >>> evens.output("out", StdOutSink())
    >>> run_main(flow)
    2
    4

    """

    trues: Stream
    falses: Stream

    def __iter__(self):
        return iter((self.trues, self.falses))


@operator(_core=True)
def branch(
    up: Stream,
    step_id: str,
    predicate: Callable[[Any], bool],
) -> BranchOut:
    """Divide items into two streams with a predicate.

    Args:
        up: Stream to divide.

        step_id: Unique ID.

        predicate: Function to call on each upstream item. Items for
            which this returns `True` will be put into one branch
            `Stream`; `False` the other branc `Stream`.h

    Returns:
        A stream of items for which the predicate returns `True`, and
        a stream of items for which the predicate returns `False`.

    """
    raise NotImplementedError()


@operator()
def count_final(up: Stream, step_id: str, key: Callable[[Any], str]) -> KeyedStream:
    """Count the number of occurrences of items in the entire stream.

    This will only return counts once the upstream is EOF. You'll need
    to use `count_window` on infinite data.

    Args:
        up: Stream of items to count.

        step_id: The name of this step.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Returns:
        A stream of `(key, count)` once the upstream is EOF.

    """
    return (
        up.map("init_count", lambda x: (key(x), 1))
        .key_assert("keyed")
        .reduce_final("sum", lambda s, x: s + x)
    )


@operator(_core=True)
def flat_map(
    up: Stream,
    step_id: str,
    mapper: Callable[[Any], Iterable[Any]],
) -> Stream:
    """Flat map is a one-to-many transformation of items.

    This is like a combination of `map.map` and `flatten.flatten`.

    It is commonly used for:

    - Tokenizing

    - Flattening hierarchical objects

    - Breaking up aggregations for further processing

    >>> from bytewax.testing import TestingSource
    >>> from bytewax.connectors.stdio import StdOutSink
    >>> from bytewax.testing import run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow()
    >>> inp = ["hello world"]
    >>> flow.input("inp", TestingSource(inp))
    >>> def split_into_words(sentence):
    ...     return sentence.split()
    >>> flow.flat_map("split_words", split_into_words)
    >>> flow.output("out", StdOutSink())
    >>> run_main(flow)
    hello
    world

    Args:
        up: Stream.

        step_id: Unique ID.

        mapper: Called once on each upstream item. Returns the items
            to emit downstream.

    Returns:
        A stream of each item in the iterable retuned by the mapper.

    """
    raise NotImplementedError()


@operator()
def flat_map_value(
    up: KeyedStream,
    step_id: str,
    mapper: Callable[[Any], Iterable[Any]],
) -> KeyedStream:
    """Flat map is a one-to-many transformation of values.

    Args:
        up: Stream.

        step_id: Unique ID.

        mapper: Called once on each upstream item. Returns the items
            to emit downstream.

    Returns:
        A stream of each item in the iterable retuned by the mapper.

    """

    def shim_mapper(k_v):
        try:
            k, v = k_v
        except TypeError as ex:
            msg = (
                f"step {step_id!r} requires `(key, value)` 2-tuple "
                "as upstream for routing; "
                f"got a {type(k_v)!r} instead"
            )
            raise TypeError(msg) from ex
        ws = mapper(v)
        return ((k, w) for w in ws)

    return up.flat_map("flat_map", shim_mapper).key_assert("keyed")


@operator()
def flatten(up: Stream, step_id: str) -> Stream:
    """Move all sub-items up a level.

    Args:
        up: Stream of iterables.

        step_id: Unique ID.

    Returns:
        A stream of the items within each iterable in the upstream.

    """

    def shim_mapper(x):
        if not isinstance(x, Iterable):
            msg = (
                f"step {step_id!r} requires upstream to be iterables; "
                f"got a {type(x)!r} instead"
            )
            raise TypeError(msg)

        return x

    return up.flat_map("flat_map", shim_mapper)


@operator()
def filter(  # noqa: A001
    up: Stream, step_id: str, predicate: Callable[[Any], bool]
) -> Stream:
    """Filter selectively keeps only some items.

    It is commonly used for:

    - Selecting relevant events

    - Removing empty events

    - Removing sentinels

    - Removing stop words

    >>> from bytewax.testing import TestingSource
    >>> from bytewax.connectors.stdio import StdOutSink
    >>> from bytewax.testing import run_main
    >>> from bytewax.dataflow import Dataflow
    >>>
    >>> flow = Dataflow()
    >>> flow.input("inp", TestingSource(range(4)))
    >>> def is_odd(item):
    ...     return item % 2 != 0
    >>> flow.filter("filter_odd", is_odd)
    >>> flow.output("out", StdOutSink())
    >>> run_main(flow)
    1
    3

    Args:
        up: Stream.

        step_id: Unique ID.

        predicate: Called with each upstream item. Only items for
            which this returns true `True` will be emitted downstream.

    Returns:
        A stream with only the upstream items for which the predicate
        returns `True`.

    """

    def shim_mapper(x):
        keep = predicate(x)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [x]

        return []

    return up.flat_map("flat_map", shim_mapper)


@operator()
def filter_value(
    up: KeyedStream, step_id: str, predicate: Callable[[Any], bool]
) -> KeyedStream:
    """Selectively keep only some items from a keyed stream.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        predicate: Will be called with each upstream value. Only
            values for which this returns `True` will be emitted
            downstream.

    Returns:
        A keyed stream with only the upstream pairs for which the
        predicate returns `True`.

    """

    def shim_mapper(v):
        keep = predicate(v)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a `bool`; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [v]

        return []

    return up.flat_map_value("filter", shim_mapper)


@operator()
def filter_map(
    up: Stream, step_id: str, mapper: Callable[[Any], Optional[Any]]
) -> Stream:
    """A one-to-maybe-one transformation of items.

    This is like a combination of `map.map` and then `filter.filter`
    with a predicate removing `None` values.

    >>> flow = Dataflow()
    >>> def validate(data):
    ...     if type(data) != dict or "key" not in data:
    ...         return None
    ...     else:
    ...         return data["key"], data
    ...
    >>> flow.filter_map("validate", validate)

    Args:
        up: Stream.

        step_id: Unique ID.

        mapper: Called on each item. Each return value is emitted
            downstream, unless it is `None`.

    Returns:
        A stream of items returned from the mapper, unless it is
        `None`.

    """

    def shim_mapper(x):
        y = mapper(x)
        if y is not None:
            return [y]

        return []

    return up.flat_map("flat_map", shim_mapper)


@dataclass
class _FoldFinalLogic(UnaryLogic):
    step_id: str
    folder: Callable[[Any, Any], Any]
    state: Any

    def on_item(self, _now: datetime, v: Any) -> Tuple[Iterable[Any], bool]:
        self.state = self.folder(self.state, v)
        return ([], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        # No need to deepcopy because we are discarding the state.
        return ([self.state], UnaryLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return copy.deepcopy(self.state)


@operator()
def fold_final(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
) -> KeyedStream:
    """Build an empty accumulator, then combine values into it.

    It is like `reduce.reduce` but uses a function to build the
    initial value.

    Args:
        up: Keyed stream.

        step_id: Unique ID.

        builder: Called the first time a key appears and is expected
            to return the empty accumulator for that key.

        folder: Combines a new value into an existing accumulator and
            returns the updated accumulator. The accumulator is
            initially the empty accumulator.

    Returns:
        A keyed stream of the completed accumulators.

    """

    def shim_builder(_now: datetime, resume_state: Optional[Any]) -> _FoldFinalLogic:
        state = resume_state if resume_state is not None else builder()
        return _FoldFinalLogic(step_id, folder, state)

    return up.unary("unary", shim_builder)


@operator(_core=True)
def input(  # noqa: A001
    flow: Dataflow,
    step_id: str,
    source: Source,
) -> Stream:
    """Introduce items into a dataflow.

    See `bytewax.inputs` for more information on how input works.
    See `bytewax.connectors` for a buffet of our built-in
    connector types.

    At least one input is required on every dataflow.

    Args:
        flow: The dataflow.

        step_id: Unique ID.

        source: Source to read items from.

    Returns:
        A stream of items from the source. See source documentation
        for what kind of item that is.

    """
    raise NotImplementedError()


def _default_inspector(step_id: str, item: Any) -> None:
    print(f"{step_id}: {item!r}", flush=True)


@operator()
def inspect(
    up: Stream, step_id: str, inspector: Callable[[str, Any], None] = _default_inspector
) -> Stream:
    """Observe items for debugging.

    >>> from bytewax.testing import TestingSource, TestingSink
    >>> from bytewax.testing import run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("my_flow")
    >>> inp = flow.input("inp", TestingSource(range(3)))
    >>> inp.inspect("help")
    >>> out = []
    >>> inp.output("out", TestingSink(out))  # Notice we don't print out.
    >>> run_main(flow)
    my_flow.help: 0
    my_flow.help: 1
    my_flow.help: 2

    Args:
        up: Stream.

        step_id: Unique ID.

        inspector: Called with the step ID and each item in the
            stream. Defaults to printing out the step_id and item.

    Returns:
        The upstream unmodified.

    """

    def shim_inspector(_fq_step_id, item, _epoch, _worker_idx):
        inspector(step_id, item)

    return up.inspect_debug("inspect_debug", shim_inspector)


def _default_debug_inspector(step_id: str, item: Any, epoch: int, worker: int) -> None:
    print(f"{step_id} W{worker} @{epoch}: {item!r}", flush=True)


@operator(_core=True)
def inspect_debug(
    up: Stream,
    step_id: str,
    inspector: Callable[[str, Any, int, int], None] = _default_debug_inspector,
) -> Stream:
    """Observe items, their worker, and their epoch for debugging.

    >>> from bytewax.testing import TestingSource, TestingSink
    >>> from bytewax.testing import run_main
    >>> from bytewax.dataflow import Dataflow
    >>> flow = Dataflow("my_flow")
    >>> inp = flow.input("inp", TestingSource(range(3)))
    >>> inp.inspect_debug("help")
    >>> out = []
    >>> inp.output("out", TestingSink(out))  # Notice we don't print out.
    >>> run_main(flow)
    my_flow.help W0 @0: 0
    my_flow.help W0 @0: 1
    my_flow.help W0 @0: 2

    Args:
        up: Stream.

        step_id: Unique ID.

        inspector: Called with the step ID, each item in the stream,
            the epoch of that item, and the worker processing the
            item. Defaults to printing out the all the arguments.

    Returns:
        The upstream unmodified.

    """
    raise NotImplementedError()


@dataclass
class _JoinState:
    seen: Dict[Any, List[Any]]

    @classmethod
    def for_names(cls, names: List[Any]) -> "_JoinState":
        return cls({name: [] for name in names})

    def set_val(self, name: Any, value: Any) -> None:
        self.seen[name] = [value]

    def add_val(self, name: Any, value: Any) -> None:
        self.seen[name].append(value)

    def is_set(self, name: Any) -> bool:
        return len(self.seen[name]) > 0

    def all_set(self) -> bool:
        return all(self.is_set(name) for name in self.seen.keys())

    def astuples(self) -> Iterable[Tuple]:
        return itertools.product(
            *(vals if len(vals) > 0 else [None] for vals in self.seen.values())
        )

    def asdicts(self) -> Iterable[Dict]:
        EMPTY = object()
        ts = itertools.product(
            *(vals if len(vals) > 0 else [EMPTY] for vals in self.seen.values())
        )
        for t in ts:
            yield dict((n, v) for n, v in zip(self.seen.keys(), t) if v is not EMPTY)


@dataclass
class _JoinLogic(UnaryLogic):
    step_id: str
    running: bool
    state: _JoinState

    def on_item(self, _now: datetime, name_value: Any) -> Tuple[Iterable[Any], bool]:
        name, value = name_value

        self.state.set_val(name, value)

        if self.running:
            return ([copy.deepcopy(self.state)], UnaryLogic.RETAIN)
        else:
            if self.state.all_set():
                # No need to deepcopy because we are discarding the state.
                return ([self.state], UnaryLogic.DISCARD)
            else:
                return ([], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return copy.deepcopy(self.state)


@operator()
def _join_name_merge(
    flow: Dataflow,
    step_id: str,
    **named_ups: Stream,
) -> KeyedStream:
    with_names = [
        # Horrible mess, see
        # https://docs.astral.sh/ruff/rules/function-uses-loop-variable/
        up.map_value(f"name_{name}", partial(lambda name, v: (name, v), name))
        for name, up in named_ups.items()
    ]
    return flow.merge_all("merge", *with_names).key_assert("keyed")


@operator()
def join(
    left: KeyedStream,
    step_id: str,
    *rights: KeyedStream,
    running: bool = False,
) -> KeyedStream:
    named_ups = dict((str(i), s) for i, s in enumerate([left] + list(rights)))
    names = list(named_ups.keys())

    def shim_builder(_now: datetime, resume_state: Optional[Any]) -> _JoinLogic:
        state = (
            resume_state if resume_state is not None else _JoinState.for_names(names)
        )
        return _JoinLogic(step_id, running, state)

    return (
        left.flow()
        ._join_name_merge("add_names", **named_ups)
        .unary("join", shim_builder)
        .flat_map_value("astuple", _JoinState.astuples)
    )


@operator()
def join_named(
    flow: Dataflow,
    step_id: str,
    running: bool = False,
    **ups: KeyedStream,
) -> KeyedStream:
    names = list(ups.keys())

    def shim_builder(_now: datetime, resume_state: Optional[Any]) -> _JoinLogic:
        state = (
            resume_state if resume_state is not None else _JoinState.for_names(names)
        )
        return _JoinLogic(step_id, running, state)

    return (
        flow._join_name_merge("add_names", **ups)
        .key_assert("keyed")
        .unary("join", shim_builder)
        .flat_map_value("asdict", _JoinState.asdicts)
    )


@operator()
def key_assert(up: Stream, step_id: str) -> KeyedStream:
    """Assert that this stream contains `(key, value)` 2-tuples.

    This allows you to use all the keyed operators that only are
    methods on `KeyedStream`.

    If the upstream does not contain 2-tuples, downstream keyed
    operators will throw exceptions.

    Args:
        up: Stream.

        step_id: Unique ID.

    Returns:
        The upstream unmodified.

    """
    return KeyedStream._assert_from(up._noop("noop"))


@operator()
def key_on(up: Stream, step_id: str, key: Callable[[Any], str]) -> KeyedStream:
    def shim_mapper(v):
        k = key(v)
        if not isinstance(k, str):
            msg = (
                f"return value of `key` {f_repr(key)} "
                f"in step {step_id!r} must be a `str`; "
                f"got a {type(k)!r} instead"
            )
            raise TypeError(msg)
        return (k, v)

    return up.map("map", shim_mapper).key_assert("keyed")


@operator()
def key_split(
    up: Stream,
    step_id: str,
    key: Callable[[Any], str],
    *values: Callable[[Any], Any],
) -> MultiStream:
    keyed_up = up.key_on("key", key)
    streams = {
        str(i): keyed_up.map_value(f"value_{str(i)}", value)
        for i, value in enumerate(values)
    }
    return MultiStream(streams)


@operator()
def map(  # noqa: A001
    up: Stream,
    step_id: str,
    mapper: Callable[[Any], Any],
) -> Stream:
    def shim_mapper(x):
        y = mapper(x)
        return [y]

    return up.flat_map("flat_map", shim_mapper)


@operator()
def map_value(
    up: KeyedStream, step_id: str, mapper: Callable[[Any], Any]
) -> KeyedStream:
    def shim_mapper(v):
        w = mapper(v)
        return [w]

    return up.flat_map_value("flat_map_value", shim_mapper)


@operator()
def max_final(
    up: KeyedStream,
    step_id: str,
    by: Callable[[Any], Any] = _identity,
) -> KeyedStream:
    return up.reduce_final("reduce_final", partial(max, key=by))


@operator(_core=True)
def merge_all(flow: Dataflow, step_id: str, *ups: Stream) -> Stream:
    raise NotImplementedError()


@operator()
def merge(left: Stream, step_id: str, *rights: Stream) -> Stream:
    return left.flow().merge_all("merge_all", left, *rights)


@operator()
def min_final(
    up: KeyedStream,
    step_id: str,
    by: Callable[[Any], Any] = _identity,
) -> KeyedStream:
    return up.reduce_final("reduce_final", partial(min, key=by))


@operator(_core=True)
def output(up: Stream, step_id: str, sink: Sink) -> None:
    raise NotImplementedError()


@operator(_core=True)
def redistribute(up: Stream, step_id: str) -> Stream:
    raise NotImplementedError()


@operator()
def reduce_final(
    up: KeyedStream,
    step_id: str,
    reducer: Callable[[Any, Any], Any],
) -> KeyedStream:
    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold_final("fold_final", _none_builder, shim_folder)


@dataclass
class _StatefulMapLogic(UnaryLogic):
    step_id: str
    mapper: Callable[[Any, Any], Tuple[Any, Iterable[Any]]]
    state: Optional[Any]

    def on_item(self, _now: datetime, v: Any) -> Tuple[Iterable[Any], bool]:
        res = self.mapper(self.state, v)
        try:
            self.state, w = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(self.mapper)} "
                f"in step {self.step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_item)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex
        return ([w], self.state is None)

    def on_notify(self, _s: datetime) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return copy.deepcopy(self.state)


@operator()
def stateful_map(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[], Any],
    mapper: Callable[[Any, Any], Tuple[Any, Iterable[Any]]],
) -> KeyedStream:
    def shim_builder(_now: datetime, resume_state: Optional[Any]) -> _StatefulMapLogic:
        state = resume_state if resume_state is not None else builder()
        return _StatefulMapLogic(step_id, mapper, state)

    return up.unary("unary", shim_builder)


@operator(_core=True)
def unary(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[datetime, Optional[Any]], UnaryLogic],
) -> KeyedStream:
    raise NotImplementedError()
