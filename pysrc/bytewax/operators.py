"""Built-in operators.

These are automatically loaded when you `import bytewax`. Operators
defined elsewhere must be loaded. See `bytewax.dataflow.load_mod_ops`
and the `bytewax.dataflow` module docstring for how to load custom
operators.

See the `bytewax.dataflow` module docstring for how to define your own
custom operators.

"""

from abc import ABC, abstractmethod
from collections import OrderedDict
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
    Type,
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
from bytewax.window import ClockConfig, WindowConfig


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
    above calls then: `notify_at`. `snapshot` will be periodically be
    called.

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
        """State to store for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of `unary.unary` when resuming.

        The state must be `pickle`-able.

        """
        ...


@operator(_core=True)
def _noop(up: Stream, step_id: str) -> Stream:
    """No-op; is compiled out when making the Timely dataflow.

    Sometimes necessary to ensure `Dataflow` structure is valid.

    """
    raise NotImplementedError()


@operator()
def assert_keyed(up: Stream, step_id: str) -> KeyedStream:
    """Assert that this stream contains `(key, value)` 2-tuples.

    This allows you to use all the keyed operators on `KeyedStream`.

    If the upstream does not contain 2-tuples, downstream keyed
    operators will throw exceptions.

    Args:
        up: Stream.

        step_id: Unique ID.

    Yields:
        The upstream unmodified.

    """
    return KeyedStream._assert_from(up._noop("shim_noop"))


@dataclass
class _BatchState:
    acc: List[Any] = field(default_factory=list)
    timeout_at: Optional[datetime] = None


@dataclass
class _BatchShimLogic(UnaryLogic):
    step_id: str
    timeout: timedelta
    batch_size: int
    state: _BatchState

    def on_item(self, now: datetime, v: Any) -> Tuple[Iterable[Any], bool]:
        self.state.timeout_at = now + self.timeout

        self.state.acc.append(v)
        if len(self.state.acc) >= self.batch_size:
            return ([self.state.acc], UnaryLogic.DISCARD)

        return ([], UnaryLogic.RETAIN)

    def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        return ([self.state.acc], UnaryLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        return self.state.timeout_at

    def snapshot(self) -> Any:
        return self.state


@operator()
def batch(
    up: KeyedStream, step_id: str, timeout: timedelta, batch_size: int
) -> KeyedStream:
    """Batch incoming items up to a size or a timeout.

    Args:
        up: Stream.

        step_id: Unique ID.

        timeout: Timeout before emitting the batch, even if max_size
            was not reached.

        batch_size: Maximum size of the batch.

    Yields:
        Batches of items gathered into a `list`.

    """

    def shim_builder(resume_state: Optional[Any]) -> UnaryLogic:
        state = resume_state if resume_state is not None else _BatchState()
        return _BatchShimLogic(step_id, timeout, batch_size, state)

    return up.unary("shim_unary", shim_builder)


@dataclass(frozen=True)
class BranchOut:
    """Streams returned from `branch` operator.

    You can tuple unpack this for convenience.

    >>> flow = Dataflow("my_flow")
    >>> nums = flow.input("nums", TestingSource([1, 2, 3]))
    >>> evens, odds = nums.split("split_even", lambda x: x % 2 == 0)

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
        up: Stream.

        step_id: Unique ID.

        predicate: Function to call on each upstream item. Items for
            which this returns `True` will be put into one branch
            `Stream`; `False` the other branc `Stream`.h

    Returns:
        A `Stream` of `True` items, and a `Stream` of `False` items.

    """
    raise NotImplementedError()


# TODO: Return another output stream with the unique `(key, obj)`
# mappings? In case you need to re-join? Or actually we could do the
# join here...
@operator()
def count_final(up: Stream, step_id: str, key: Callable[[Any], str]) -> KeyedStream:
    """Count the number of occurrences of items in the entire stream.

    This will only return counts once the upstream is EOF. You'll need
    to use `count_window` on infinite data.

    Args:
        up: Stream.

        step_id: The name of this step.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Yields:
        `(key, count)`

    """
    return (
        up.map("key", lambda x: (key(x), 1))
        .assert_keyed("shim_assert")
        .reduce("sum", lambda s, x: s + x, _never_complete, eof_is_complete=True)
    )


@operator()
def count_window(
    up: Stream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    key: Callable[[Any], str],
) -> KeyedStream:
    """Count the number of occurrences of items in a window.

    This will only return counts once the window has closed.

    Args:
        up: Stream.

        step_id: Unique ID.

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Yields:
        `(key, count)` per window.

    """
    return (
        up.map("key", lambda x: (key(x), 1))
        .assert_keyed("shim_assert")
        .reduce_window("sum", clock, windower, lambda s, x: s + x)
    )


@operator(_core=True)
def flat_map(
    up: Stream,
    step_id: str,
    mapper: Callable[[Any], Iterable[Any]],
) -> Stream:
    raise NotImplementedError()


@operator()
def flatten(up: Stream, step_id: str) -> Stream:
    return up.flat_map("shim_flat_map", lambda xs: xs)


@operator()
def filter(  # noqa: A001
    up: Stream, step_id: str, predicate: Callable[[Any], bool]
) -> Stream:
    def shim_mapper(x):
        keep = predicate(x)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {f_repr(predicate)} "
                f"in step {step_id!r} must be a bool; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [x]

        return []

    return up.flat_map("shim_flat_map", shim_mapper)


@operator()
def filter_value(
    up: KeyedStream, step_id: str, predicate: Callable[[Any], bool]
) -> KeyedStream:
    def shim_predicate(k_v):
        try:
            k, v = k_v
        except TypeError as ex:
            msg = (
                f"step {step_id!r} requires `(key, value)` 2-tuple "
                "as upstream for routing; "
                f"got a {type(k_v)!r} instead"
            )
            raise TypeError(msg) from ex
        return predicate(v)

    return up.filter("shim_filter", shim_predicate).assert_keyed("shim_assert")


@operator()
def filter_map(
    up: Stream, step_id: str, mapper: Callable[[Any], Optional[Any]]
) -> Stream:
    def shim_mapper(x):
        y = mapper(x)
        if y is not None:
            return [y]

        return []

    return up.flat_map("shim_flat_map", shim_mapper)


@dataclass
class _FoldShimLogic(UnaryLogic):
    step_id: str
    folder: Callable[[Any, Any], Any]
    is_fold_complete: Callable[[Any], bool]
    eof_is_complete: bool
    state: Any

    def on_item(self, _now: datetime, v: Any) -> Tuple[Iterable[Any], bool]:
        self.state = self.folder(self.state, v)

        is_c = self.is_fold_complete(self.state)
        if not isinstance(is_c, bool):
            msg = (
                f"return value of `is_complete` {f_repr(self.is_fold_complete)} "
                f"in step {self.step_id!r} must be a bool; "
                f"got a {type(is_c)!r} instead"
            )
            raise TypeError(msg)
        if is_c:
            return ([self.state], UnaryLogic.DISCARD)

        return ([], UnaryLogic.RETAIN)

    def on_notify(self, _s: datetime) -> Tuple[Iterable[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[Iterable[Any], bool]:
        if self.eof_is_complete:
            return ([self.state], UnaryLogic.DISCARD)

        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return self.state


@operator()
def fold(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
    is_complete: Callable[[Any], bool],
    eof_is_complete: bool = True,
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryLogic:
        state = resume_state if resume_state is not None else builder()
        return _FoldShimLogic(step_id, folder, is_complete, eof_is_complete, state)

    return up.unary("shim_unary", shim_builder)


@operator(_core=True)
def fold_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
) -> KeyedStream:
    raise NotImplementedError()


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
def collect(
    up: KeyedStream,
    step_id: str,
    is_complete: Callable[[Any], bool],
    into: Type = list,
    eof_is_complete: bool = True,
) -> KeyedStream:
    collector = _get_collector(into)

    return up.fold("shim_fold", into, collector, is_complete, eof_is_complete)


@operator()
def collect_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type = list,
) -> KeyedStream:
    collector = _get_collector(into)

    return up.fold_window("shim_fold_window", clock, windower, into, collector)


@operator(_core=True)
def input(  # noqa: A001
    flow: Dataflow,
    step_id: str,
    source: Source,
) -> Stream:
    """Emits items downstream from an input source.

    See `bytewax.inputs` for more information on how input works.
    See `bytewax.connectors` for a buffet of our built-in
    connector types.

    At least one input is required on every dataflow.


    Args:
        step_id:

            Uniquely identifies this step for recovery.

        source:

            `Source` to read items from.

    Yields:
        Items from the source. See source documentation for what kind
        of item that is.

    """
    raise NotImplementedError()


@operator()
def inspect(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any], None] = None,  # type: ignore[assignment]
) -> Stream:
    if inspector is None:

        def inspector(x):
            print(f"{step_id}: {x!r}")

    def shim_inspector(x, _epoch, _worker_idx):
        inspector(x)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@operator(_core=True)
def inspect_debug(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any, int, int], None],
) -> Stream:
    raise NotImplementedError()


@operator()
def inspect_epoch(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any, int], None] = None,  # type: ignore[assignment]
) -> Stream:
    if inspector is None:

        def inspector(x, epoch):
            print(f"{step_id} @{epoch}: {x!r}")

    def shim_inspector(x, epoch, _worker_idx):
        inspector(x, epoch)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@operator()
def inspect_worker(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any, int], None] = None,  # type: ignore[assignment]
) -> Stream:
    if inspector is None:

        def inspector(x, worker_idx):
            print(f"{step_id} on W{worker_idx}: {x!r}")

    def shim_inspector(x, _epoch, worker_idx):
        inspector(x, worker_idx)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@dataclass
class _JoinState:
    keys: List[Any]
    vals: Dict[Any, Any] = field(default_factory=dict)

    def set_val(self, key: str, value: Any) -> None:
        self.vals[key] = value

    def is_set(self, key: str) -> bool:
        return key in self.vals

    def all_set(self) -> bool:
        return all(key in self.vals for key in self.keys)

    def astuple(self) -> Tuple:
        return tuple(self.vals[key] for key in self.keys)

    def asdict(self) -> OrderedDict:
        return OrderedDict((key, self.vals[key]) for key in self.keys)


def _int_dict_to_tuple(d: Dict[str, Any]) -> Tuple:
    return tuple(d[str(i)] for i in range(len(d)))


@operator()
def join_inner(
    left: KeyedStream,
    step_id: str,
    running: bool = False,
    *rights: KeyedStream,
) -> KeyedStream:
    tables = {str(i + 1): right for i, right in enumerate(rights)}
    # Keep things in argument order.
    tables["0"] = left
    return (
        left.flow()
        .join_inner_named("shim_join_inner_named", running=running, **tables)
        .map_value("unname", _int_dict_to_tuple)
    )


def _join_inner_folder(s: _JoinState, k_v):
    k, v = k_v
    s.set_val(k, v)
    return s


@operator()
def join_inner_named(
    # Extend `Dataflow` so we can get all the "table names" as
    # kwargs.
    flow: Dataflow,
    step_id: str,
    running: bool = False,
    **tables: KeyedStream,
) -> KeyedStream:
    ups = [
        stream.map_value(f"inject_key_{key}", partial(lambda v, key: (key, v), key))
        for key, stream in tables.items()
    ]
    keys = list(tables.keys())

    return (
        flow.merge_all("merge_ups", *ups)
        .fold("join", lambda: _JoinState(keys), _join_inner_folder, _JoinState.all_set)
        .map_value("make_dict", _JoinState.asdict)
    )


@operator()
def join_inner_window(
    left: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    right: KeyedStream,
) -> KeyedStream:
    return (
        left._keyed_idx("add_idx", right)
        .fold_window(
            "shim_fold_window",
            clock,
            windower,
            lambda: _JoinState(list(range(2))),
            _join_inner_folder,
        )
        .map_value("make_tuple", _JoinState.astuple)
    )


@operator()
def key_on(up: Stream, step_id: str, key: Callable[[Any], str]) -> KeyedStream:
    def shim_mapper(v):
        k = key(v)
        return (k, v)

    return up.map("shim_map", shim_mapper).assert_keyed("shim_assert")


@operator()
def map(  # noqa: A001
    up: Stream,
    step_id: str,
    mapper: Callable[[Any], Any],
) -> Stream:
    def shim_mapper(x):
        y = mapper(x)
        return [y]

    return up.flat_map("shim_flat_map", shim_mapper)


@operator()
def map_value(
    up: KeyedStream, step_id: str, mapper: Callable[[Any], Any]
) -> KeyedStream:
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
        w = mapper(v)
        return (k, w)

    return up.map("shim_map", shim_mapper).assert_keyed("shim_assert")


@operator()
def max_final(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce("shim_reduce", max, _never_complete, eof_is_complete=True)


@operator()
def max_window(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, max)


@operator(_core=True)
def merge_all(flow: Dataflow, step_id: str, *ups: Stream) -> Stream:
    raise NotImplementedError()


@operator()
def merge(left: Stream, step_id: str, *rights: Stream) -> Stream:
    return left.flow().merge_all("shim_merge_all", left, *rights)


@operator()
def min_final(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce("shim_reduce", min, _never_complete, eof_is_complete=True)


@operator()
def min_window(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, min)


@operator(_core=True)
def output(up: Stream, step_id: str, sink: Sink) -> None:
    raise NotImplementedError()


@operator(_core=True)
def redistribute(up: Stream, step_id: str) -> Stream:
    raise NotImplementedError()


@operator()
def reduce(
    up: KeyedStream,
    step_id: str,
    reducer: Callable[[Any, Any], Any],
    is_complete: Callable[[Any], bool],
    eof_is_complete: bool = True,
) -> KeyedStream:
    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold(
        "shim_fold", _none_builder, shim_folder, is_complete, eof_is_complete
    )


@operator()
def reduce_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    reducer: Callable[[Any, Any], Any],
) -> KeyedStream:
    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold_window(
        "shim_fold_window", clock, windower, _none_builder, shim_folder
    )


@operator()
def split(
    up: Stream,
    step_id: str,
    key: Callable[[Any], str],
    **getters: Callable[[Any], Any],
) -> MultiStream:
    keyed_up = up.key_on("shim_key_on", key)
    streams = {
        name: keyed_up.map_value(f"map_value_{name}", getter)
        for name, getter in getters.items()
    }
    return MultiStream(streams)


@dataclass
class _StatefulMapShimLogic(UnaryLogic):
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
        return self.state


@operator()
def stateful_map(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[], Any],
    mapper: Callable[[Any, Any], Tuple[Any, Iterable[Any]]],
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryLogic:
        state = resume_state if resume_state is not None else builder()
        return _StatefulMapShimLogic(step_id, mapper, state)

    return up.unary("shim_unary", shim_builder)


@operator(_core=True)
def unary(
    up: KeyedStream,
    step_id: str,
    builder: Callable[[Optional[Any]], UnaryLogic],
) -> KeyedStream:
    raise NotImplementedError()
