"""Built-in operators.

See `bytewax.dataflow` module docstring for how to define custom
operators.

"""

from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
)

from bytewax.dataflow import Dataflow, KeyedStream, Stream, f_repr, operator
from bytewax.inputs import Source
from bytewax.outputs import Sink
from bytewax.window import ClockConfig, WindowConfig


class UnaryLogic(ABC):
    """Abstract class to define the behavior of a `unary` operator.

    The operator will call these methods in order: `on_item` once for
    any items queued, then `on_notify` if the notification time has
    passed, then `on_eof` if the upstream is EOF and no new items will
    be received this execution, then `is_complete` to determine if
    this logic should be discarded, then if it is retained
    `notify_at`, then `snapshot` will be periodically be called.

    """

    @abstractmethod
    def on_item(self, now: datetime, value: Any) -> Iterable[Any]:
        """Called on each new upstream item.

        This will be called multiple times in a row if there are
        multiple items from upstream.

        Args:
            now: The current `datetime`.

            value: The value of the upstream `(key, value)`.

        Returns:
            Any values to emit downstream. They will be wrapped in
            `(key, value)` automatically.

        """
        ...

    @abstractmethod
    def on_notify(self, sched: datetime) -> Iterable[Any]:
        """Called when the scheduled notification time has passed.

        Args:
            sched: The scheduled notification time.

        Returns:
            Any values to emit downstream. They will be wrapped in
            `(key, value)` automatically.

        """
        ...

    @abstractmethod
    def on_eof(self) -> Iterable[Any]:
        """The upstream has no more items on this execution.

        This will only be called once per execution after `on_item` is
        done being called.

        Returns:
            Any values to emit downstream. They will be wrapped in
            `(key, value)` automatically.

        """
        ...

    @abstractmethod
    def is_complete(self) -> bool:
        """Should this logic should be deleted?

        Usually this requires having an instance variable you mark in
        `on_*` when this logic is no longer needed.

        This will be called if any of the `on_*` methods were called.

        It will be deleted right after this call.

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
        function of `unary` when resuming.

        The state must be `pickle`-able.

        """
        ...


@operator(_core=True)
def _noop(up: Stream, step_id: str) -> Stream:
    """No-op; is compiled out when making the Timely dataflow.

    Sometimes necessary to ensure `Dataflow` structure is valid.

    """
    return up._scope._new_stream("down")


@operator()
def assert_keyed(up: Stream, step_id: str) -> KeyedStream:
    """Mark that this stream contains `(key, value)` 2-tuples."""
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
    state: Optional[_BatchState]

    def on_item(self, now: datetime, v: Any) -> Iterable[Any]:
        self.state.timeout_at = now + self.state.timeout

        self.state.acc.append(v)
        if len(self.state.acc) >= self.batch_size:
            acc = self.state.acc
            self.state = None
            return [acc]

        return []

    def on_notify(self, sched: datetime) -> Iterable[Any]:
        acc = self.state.acc
        self.state = None
        return [acc]

    def on_eof(self) -> Iterable[Any]:
        acc = self.state.acc
        self.state = None
        return [acc]

    def is_complete(self) -> bool:
        return self.state is None

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
        return (self.trues, self.falses)


@operator(_core=True)
def branch(
    up: Stream,
    step_id: str,
    predicate: Callable[[Any], bool],
) -> BranchOut:
    return BranchOut(up._scope._new_stream("trues"), up._scope._new_stream("falses"))


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

        key: Function to convert each item into a string key. The
            counting machinery does not compare the items directly,
            instead it groups by this string key.

    Yields:
        `(key, count)`

    """

    def never_complete(_):
        return False

    return (
        up.map("key", lambda x: (key(x), 1))
        .assert_keyed("shim_assert")
        .reduce("sum", lambda s, x: s + x, never_complete, eof_is_complete=True)
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
    return up._scope._new_stream("down")


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
class _FoldState:
    """State wrapper so that `None` is a valid accumulator.

    The only way to signal that a folder is done is `is_complete`
    returning `True`.

    """

    acc: Any


@dataclass
class _FoldShimLogic(UnaryLogic):
    step_id: str
    builder: Callable[[], Any]
    folder: Callable[[Any, Any], Any]
    is_fold_complete: Callable[[Any], bool]
    eof_is_complete: bool
    state: Optional[_FoldState]

    def on_item(self, _now: datetime, v: Any) -> Iterable[Any]:
        if self.state is None:
            self.state = _FoldState(self.builder())

        self.state.acc = self.folder(self.state.acc, v)

        is_c = self.is_fold_complete(self.state.acc)
        if not isinstance(is_c, bool):
            msg = (
                f"return value of `is_complete` {f_repr(self.is_fold_complete)} "
                f"in step {self.step_id!r} must be a bool; "
                f"got a {type(is_c)!r} instead"
            )
            raise TypeError(msg)
        if is_c:
            acc = self.state.acc
            self.state = None
            return [acc]

        return []

    def on_notify(self, _s: datetime) -> Iterable[Any]:
        return []

    def on_eof(self) -> Iterable[Any]:
        if self.eof_is_complete:
            acc = self.state.acc
            self.state = None
            return [acc]

        return []

    def is_complete(self) -> bool:
        return self.state is None

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
        return _FoldShimLogic(
            step_id, builder, folder, is_complete, eof_is_complete, resume_state
        )

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
    return KeyedStream._assert_from(up._scope._new_stream("down"))


def _get_collect_folder(t: Type) -> Callable:
    if t is list:

        def shim_folder(s, v):
            s.append(v)
            return s

    elif t is set:

        def shim_folder(s, v):
            s.add(v)
            return s

    elif t is dict:

        def shim_folder(s, k_v):
            k, v = k_v
            s[k] = v
            return s

    else:
        msg = (
            f"collect doesn't support `{t:!}`; "
            "only `list`, `set`, and `dict`; use `fold` operator directly"
        )
        raise TypeError(msg)
    return shim_folder


@operator()
def collect(
    up: KeyedStream,
    step_id: str,
    is_complete: Callable[[Any], bool],
    into: Type = list,
    eof_is_complete: bool = True,
) -> KeyedStream:
    shim_folder = _get_collect_folder(into)

    return up.fold("shim_fold", into, shim_folder, is_complete, eof_is_complete)


@operator()
def collect_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type = list,
) -> KeyedStream:
    shim_folder = _get_collect_folder(into)

    return up.fold_window("shim_fold_window", clock, windower, into, shim_folder)


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

            Source to read items from.

    Returns:
        Single stream.
    """
    return flow._scope._new_stream("down")


@operator()
def inspect(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any], None] = None,
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
    return up._scope._new_stream("down")


@operator()
def inspect_epoch(
    up: Stream,
    step_id: str,
    inspector: Callable[[Any, int], None] = None,
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
    inspector: Callable[[Any, int], None] = None,
) -> Stream:
    if inspector is None:

        def inspector(x, worker_idx):
            print(f"{step_id} on W{worker_idx}: {x!r}")

    def shim_inspector(x, _epoch, worker_idx):
        inspector(x, worker_idx)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@dataclass
class _JoinState:
    slots: List[Any] = field(init=False)
    is_set: List[bool] = field(init=False)
    arity: InitVar[int]

    def __post_init__(self, arity: int):
        self.slots = [None] * arity
        self.is_set = [False] * arity

    def _set(self, idx: int, x: Any):
        self.slots[idx] = x
        self.is_set[idx] = True

    def all_set(self) -> bool:
        return all(self.is_set)

    def astuple(self) -> Tuple:
        return tuple(self.slots)


@operator()
def _keyed_idx(
    left: KeyedStream,
    step_id: str,
    right: KeyedStream,
) -> KeyedStream:
    def build_inject_idx(i):
        def inject_idx(v):
            return (i, v)

        return inject_idx

    left = left.map_value("label_left", build_inject_idx(0))
    right = right.map_value("label_right", build_inject_idx(1))
    return left.merge("merge", right).assert_keyed("shim_assert")


@operator()
def join_inner(
    left: KeyedStream,
    step_id: str,
    right: KeyedStream,
    _is_complete: Callable[[_JoinState], bool] = _JoinState.all_set,
) -> Stream:
    def shim_folder(s, i_v):
        i, v = i_v
        s._set(i, v)
        return s

    return (
        left._keyed_idx("add_idx", right)
        .fold("shim_fold", lambda: _JoinState(2), shim_folder, _is_complete)
        .map_value("make_tuple", _JoinState.astuple)
    )


@operator()
def join_inner_window(
    left: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    right: KeyedStream,
) -> Stream:
    def shim_folder(s, i_v):
        i, v = i_v
        s._set(i, v)
        return s

    return (
        left._keyed_idx("add_idx", right)
        .fold_window(
            "shim_fold_window", clock, windower, lambda: _JoinState(2), shim_folder
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
    def never_complete(_):
        return False

    return up.reduce("shim_reduce", max, never_complete, eof_is_complete=True)


@operator()
def max_window(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, max)


# TODO: Handle *rights multiple merge. This requires changes in build
# the `Input` inner dataclass.
@operator(_core=True)
def merge(left: Stream, step_id: str, right: Stream) -> Stream:
    return left._scope._new_stream("down")


@operator()
def min_final(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    def never_complete(_):
        return False

    return up.reduce("shim_reduce", min, never_complete, eof_is_complete=True)


@operator()
def min_window(
    up: KeyedStream, step_id: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, min)


@operator(_core=True)
def output(up: Stream, step_id: str, sink: Sink) -> None:
    return None


@operator(_core=True)
def redistribute(up: Stream, step_id: str) -> Stream:
    return up._scope._new_stream("down")


@operator()
def reduce(
    up: KeyedStream,
    step_id: str,
    reducer: Callable[[Any, Any], Any],
    is_complete: Callable[[Any], bool],
    eof_is_complete: bool = True,
) -> KeyedStream:
    def shim_builder():
        return None

    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold("shim_fold", shim_builder, shim_folder, is_complete, eof_is_complete)


@operator()
def reduce_window(
    up: KeyedStream,
    step_id: str,
    clock: ClockConfig,
    windower: WindowConfig,
    reducer: Callable[[Any, Any], Any],
) -> KeyedStream:
    def shim_builder():
        return None

    def shim_folder(s, v):
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return up.fold_window(
        "shim_fold_window", clock, windower, shim_builder, shim_folder
    )


@dataclass
class _StatefulMapShimLogic(UnaryLogic):
    step_id: str
    mapper: Callable[[Any, Any], Tuple[Any, Iterable[Any]]] = field(repr=False)
    state: Optional[Any]

    def on_item(self, _now: datetime, v: Any) -> Iterable[Any]:
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
        return [w]

    def on_notify(self, _s: datetime) -> Iterable[Any]:
        return []

    def on_eof(self) -> Iterable[Any]:
        return []

    def is_complete(self) -> bool:
        return self.state is None

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
    return KeyedStream._assert_from(up._scope._new_stream("down"))
