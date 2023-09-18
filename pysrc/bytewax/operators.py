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

from bytewax.dataflow import Dataflow, Stream, Unpackable, _f_repr, operator
from bytewax.inputs import Source
from bytewax.outputs import Sink
from bytewax.window import ClockConfig, WindowConfig

__all__ = [
    "KeyedStream",
    "UnaryLogic",
    "UnaryNotifyLogic",
    "assert_keyed",
    "batch",
    "collect",
    "collect_window",
    "count_final",
    "count_window",
    "filter",
    "filter_map",
    "filter_value",
    "flat_map",
    "fold",
    "fold_window",
    "input",
    "inspect",
    "inspect_debug",
    "inspect_epoch",
    "inspect_worker",
    "join_inner",
    "join_inner_window",
    "key_on",
    "map",
    "map_value",
    "max_final",
    "max_window",
    "merge",
    "min_final",
    "min_window",
    "output",
    "redistribute",
    "reduce",
    "reduce_window",
    "split",
    "stateful_flat_map",
    "stateful_map",
    "unary",
    "unary_notify",
]


class UnaryNotifyLogic(ABC):
    """Define the behavior of a `unary_notify` operator.

    The operator will call these methods in the order defined here:
    `on_item` once for any items, `on_notify` if the notification time
    has passed, `on_eof` if the upstream is EOF, `is_complete` to
    determine if this logic should be discarded, then if it is
    retained `notify_at` to determine the next awake time, then
    finally `snapshot` will be periodically be called.

    """

    @abstractmethod
    def on_item(self, now: datetime, value: Any) -> Iterable[Any]:
        ...

    @abstractmethod
    def on_notify(self, sched: datetime) -> Iterable[Any]:
        ...

    @abstractmethod
    def on_eof(self) -> Iterable[Any]:
        ...

    @abstractmethod
    def is_complete(self) -> bool:
        ...

    @abstractmethod
    def notify_at(self) -> Optional[datetime]:
        ...

    @abstractmethod
    def snapshot(self) -> Any:
        ...


class UnaryLogic(ABC):
    @abstractmethod
    def on_item(self, value: Any) -> Iterable[Any]:
        ...

    @abstractmethod
    def on_eof(self) -> Iterable[Any]:
        ...

    @abstractmethod
    def is_complete(self) -> bool:
        ...

    @abstractmethod
    def snapshot(self) -> Any:
        ...


@operator(_core=True)
def _noop(up: Stream, step_name: str) -> Stream:
    return up._scope._new_stream("down")


@dataclass(frozen=True)
class KeyedStream(Stream):
    @classmethod
    def _assert_from(cls, stream: Stream) -> "KeyedStream":
        return KeyedStream(stream.stream_id, stream._scope)

    @classmethod
    def _help_msg(cls) -> str:
        return (
            "use `key_on` to add a key or "
            "`assert_keyed` if the stream is already a `(key, value)`"
        )


@operator()
def assert_keyed(up: Stream, step_name: str) -> KeyedStream:
    return KeyedStream._assert_from(up._noop("shim_noop"))


@dataclass
class _BatchState:
    acc: List[Any] = field(default_factory=list)
    timeout_at: Optional[datetime] = None


@dataclass
class _BatchShimLogic(UnaryNotifyLogic):
    timeout: timedelta = field(repr=False)
    batch_size: int = field(repr=False)
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
    up: KeyedStream, step_name: str, timeout: timedelta, batch_size: int
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryNotifyLogic:
        state = resume_state if resume_state is not None else _BatchState()
        return _BatchShimLogic(timeout, batch_size, state)

    return up.unary_notify("shim_unary_notify", shim_builder)


@operator()
def count_final(up: Stream, step_name: str, key: Callable[[Any], str]) -> KeyedStream:
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
    step_name: str,
    clock: ClockConfig,
    windower: WindowConfig,
    key: Callable[[Any], str],
) -> KeyedStream:
    return (
        up.map("key", lambda x: (key(x), 1))
        .assert_keyed("shim_assert")
        .reduce_window("sum", clock, windower, lambda s, x: s + x)
    )


@operator(_core=True)
def flat_map(
    up: Stream,
    step_name: str,
    mapper: Callable[[Any], Iterable[Any]],
) -> Stream:
    return up._scope._new_stream("down")


@operator()
def flatten(up: Stream, step_name: str) -> Stream:
    return up.flat_map("shim_flat_map", lambda xs: xs)


@operator()
def filter(  # noqa: A001
    up: Stream, step_name: str, predicate: Callable[[Any], bool]
) -> Stream:
    def shim_mapper(x):
        keep = predicate(x)
        if not isinstance(keep, bool):
            msg = (
                f"return value of `predicate` {_f_repr(predicate)} "
                f"in step {step_name!r} must be a bool; "
                f"got a {type(keep)!r} instead"
            )
            raise TypeError(msg)
        if keep:
            return [x]

        return []

    return up.flat_map("shim_flat_map", shim_mapper)


@operator()
def filter_value(
    up: KeyedStream, step_name: str, predicate: Callable[[Any], bool]
) -> KeyedStream:
    def shim_predicate(k_v):
        try:
            k, v = k_v
        except TypeError as ex:
            msg = (
                f"step {step_name!r} requires `(key, value)` 2-tuple "
                "as upstream for routing; "
                f"got a {type(k_v)!r} instead"
            )
            raise TypeError(msg) from ex
        return predicate(v)

    return up.filter("shim_filter", shim_predicate).assert_keyed("shim_assert")


@operator()
def filter_map(
    up: Stream, step_name: str, mapper: Callable[[Any], Optional[Any]]
) -> Stream:
    def shim_mapper(x):
        x = mapper(x)
        if x is not None:
            return [x]

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
    step_name: str = field(repr=False)
    builder: Callable[[], Any] = field(repr=False)
    folder: Callable[[Any, Any], Any] = field(repr=False)
    is_fold_complete: Callable[[Any], bool] = field(repr=False)
    eof_is_complete: bool = field(repr=False)
    state: Optional[_FoldState]

    def on_item(self, v: Any) -> Iterable[Any]:
        if self.state is None:
            self.state = _FoldState(self.builder())

        self.state.acc = self.folder(self.state.acc, v)

        done = self.is_fold_complete(self.state.acc)
        if not isinstance(done, bool):
            msg = (
                f"return value of `is_complete` {_f_repr(self.is_fold_complete)} "
                f"in step {self.step_name!r} must be a bool; "
                f"got a {type(done)!r} instead"
            )
            raise TypeError(msg)
        if done:
            acc = self.state.acc
            self.state = None
            return [acc]

        return []

    def on_eof(self) -> Iterable[Any]:
        if self.eof_is_complete:
            acc = self.state.acc
            self.state = None
            return [acc]

        return []

    def is_complete(self) -> bool:
        return self.state is None

    def snapshot(self) -> Any:
        return self.state


@operator()
def fold(
    up: KeyedStream,
    step_name: str,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
    is_complete: Callable[[Any], bool],
    eof_is_complete: bool = True,
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryLogic:
        return _FoldShimLogic(
            step_name, builder, folder, is_complete, eof_is_complete, resume_state
        )

    return up.unary("shim_unary", shim_builder)


@operator(_core=True)
def fold_window(
    up: KeyedStream,
    step_name: str,
    clock: ClockConfig,
    windower: WindowConfig,
    builder: Callable[[], Any],
    folder: Callable[[Any, Any], Any],
) -> KeyedStream:
    down = up._scope._new_stream("down")
    return KeyedStream(down.stream_id, down._scope)


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
    step_name: str,
    is_complete: Callable[[Any], bool],
    into: Type = list,
    eof_is_complete: bool = True,
) -> KeyedStream:
    shim_folder = _get_collect_folder(into)

    return up.fold("shim_fold", into, shim_folder, is_complete, eof_is_complete)


@operator()
def collect_window(
    up: KeyedStream,
    step_name: str,
    clock: ClockConfig,
    windower: WindowConfig,
    into: Type = list,
) -> KeyedStream:
    shim_folder = _get_collect_folder(into)

    return up.fold_window("shim_fold_window", clock, windower, into, shim_folder)


@operator(_core=True)
def input(  # noqa: A001
    flow: Dataflow,
    step_name: str,
    source: Source,
) -> Stream:
    """Emits items downstream from an input source.

    See `bytewax.inputs` for more information on how input works.
    See `bytewax.connectors` for a buffet of our built-in
    connector types.

    At least one input is required on every dataflow.


    Args:
        step_name:

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
    step_name: str,
    inspector: Callable[[Any], None] = None,
) -> Stream:
    if inspector is None:

        def inspector(x):
            print(f"{step_name}: {x!r}")

    def shim_inspector(x, _epoch, _worker_idx):
        inspector(x)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@operator(_core=True)
def inspect_debug(
    up: Stream,
    step_name: str,
    inspector: Callable[[Any, int, int], None],
) -> Stream:
    return up._scope._new_stream("down")


@operator()
def inspect_epoch(
    up: Stream,
    step_name: str,
    inspector: Callable[[Any, int], None] = None,
) -> Stream:
    if inspector is None:

        def inspector(x, epoch):
            print(f"{step_name} @{epoch}: {x!r}")

    def shim_inspector(x, epoch, _worker_idx):
        inspector(x, epoch)

    return up.inspect_debug("shim_inspect_debug", shim_inspector)


@operator()
def inspect_worker(
    up: Stream,
    step_name: str,
    inspector: Callable[[Any, int], None] = None,
) -> Stream:
    if inspector is None:

        def inspector(x, worker_idx):
            print(f"{step_name} on W{worker_idx}: {x!r}")

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
    step_name: str,
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
    step_name: str,
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
    step_name: str,
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
def key_on(up: Stream, step_name: str, key: Callable[[Any], str]) -> KeyedStream:
    def shim_mapper(v):
        k = key(v)
        return (k, v)

    return up.map("shim_map", shim_mapper).assert_keyed("shim_assert")


@operator()
def map(  # noqa: A001
    up: Stream,
    step_name: str,
    mapper: Callable[[Any], Any],
) -> Stream:
    def shim_mapper(x):
        x = mapper(x)
        return [x]

    return up.flat_map("shim_flat_map", shim_mapper)


@operator()
def map_value(
    up: KeyedStream, step_name: str, mapper: Callable[[Any], Any]
) -> KeyedStream:
    def shim_mapper(k_v):
        try:
            k, v = k_v
        except TypeError as ex:
            msg = (
                f"step {step_name!r} requires `(key, value)` 2-tuple "
                "as upstream for routing; "
                f"got a {type(k_v)!r} instead"
            )
            raise TypeError(msg) from ex
        v = mapper(v)
        return (k, v)

    return up.map("shim_map", shim_mapper).assert_keyed("shim_assert")


@operator()
def max_final(
    up: KeyedStream, step_name: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    def never_complete(_):
        return False

    return up.reduce("shim_reduce", max, never_complete, eof_is_complete=True)


@operator()
def max_window(
    up: KeyedStream, step_name: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, max)


# TODO: Handle *rights multiple merge. This requires changes in build
# the `Input` inner dataclass.
@operator(_core=True)
def merge(left: Stream, step_name: str, right: Stream) -> Stream:
    return left._scope._new_stream("down")


@operator()
def min_final(
    up: KeyedStream, step_name: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    def never_complete(_):
        return False

    return up.reduce("shim_reduce", min, never_complete, eof_is_complete=True)


@operator()
def min_window(
    up: KeyedStream, step_name: str, clock: ClockConfig, windower: WindowConfig
) -> KeyedStream:
    return up.reduce_window("shim_reduce_window", clock, windower, min)


@operator(_core=True)
def output(up: Stream, step_name: str, sink: Sink) -> None:
    return None


@operator(_core=True)
def redistribute(up: Stream, step_name: str) -> Stream:
    return up._scope._new_stream("down")


@operator()
def reduce(
    up: KeyedStream,
    step_name: str,
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
    step_name: str,
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


@dataclass(frozen=True)
class SplitOut(Unpackable):
    trues: Stream
    falses: Stream


@operator(_core=True)
def split(
    up: Stream,
    step_name: str,
    predicate: Callable[[Any], bool],
) -> SplitOut:
    return SplitOut(up._scope._new_stream("trues"), up._scope._new_stream("falses"))


@dataclass
class _StatefulFlatMapShimLogic(UnaryLogic):
    step_name: str
    mapper: Callable[[Optional[Any], Any], Tuple[Optional[Any], Iterable[Any]]] = field(
        repr=False
    )
    state: Optional[Any]

    def on_item(self, v: Any) -> Iterable[Any]:
        res = self.mapper(self.state, v)
        try:
            self.state, vs = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {_f_repr(self.mapper)} "
                f"in step {self.step_name!r} "
                "must be a 2-tuple of `(updated_state, emit_items)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex

        try:
            if isinstance(vs, str):
                msg = (
                    "returning a `str` for `emit_items` would emit each character; "
                    "you almost assuredly did't want that; "
                    "if you do, turn it into a `list(s)`"
                )
                raise TypeError(msg)

            return vs
        except TypeError as ex:
            msg = (
                f"2nd return value of `mapper` {_f_repr(self.mapper)} "
                f"in step {self.step_name!r} "
                "must be an iterable `emit_items`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex

    def on_eof(self) -> Iterable[Any]:
        return []

    def is_complete(self) -> bool:
        return self.state is None

    def snapshot(self) -> Any:
        return self.state


@operator()
def stateful_flat_map(
    up: KeyedStream,
    step_name: str,
    mapper: Callable[[Optional[Any], Any], Tuple[Optional[Any], Iterable[Any]]],
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryLogic:
        return _StatefulFlatMapShimLogic(step_name, mapper, resume_state)

    return up.unary("shim_unary", shim_builder)


@operator()
def stateful_map(
    up: KeyedStream,
    step_name: str,
    mapper: Callable[[Optional[Any], Any], Tuple[Optional[Any], Any]],
) -> KeyedStream:
    def shim_mapper(s, v):
        res = mapper(s, v)
        try:
            s, v = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {_f_repr(mapper)} in step {step_name!r} "
                "must be a 2-tuple of `(updated_state, emit_item)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex
        return (s, [v])

    return up.stateful_flat_map("shim_statful_flat_map", shim_mapper)


@dataclass
class _UnaryShimLogic(UnaryNotifyLogic):
    inner: UnaryLogic

    def on_item(self, _now: datetime, value: Any) -> Iterable[Any]:
        return self.inner.on_item(value)

    def on_notify(self, _sched: datetime) -> Iterable[Any]:
        return []

    def on_eof(self) -> Iterable[Any]:
        return self.inner.on_eof()

    def is_complete(self) -> bool:
        return self.inner.is_complete()

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return self.inner.snapshot()


@operator()
def unary(
    up: KeyedStream,
    step_name: str,
    builder: Callable[[Optional[Any]], UnaryLogic],
) -> KeyedStream:
    def shim_builder(resume_state: Optional[Any]) -> UnaryNotifyLogic:
        inner = builder(resume_state)
        if not isinstance(inner, UnaryLogic):
            msg = (
                f"return value of `builder` {_f_repr(builder)} "
                f"in step {step_name!r} must be a `bytewax.operators.UnaryLogic`; "
                f"got {type(inner)!r} instead"
            )
            raise TypeError(msg)
        return _UnaryShimLogic(inner)

    return up.unary_notify("shim_unary_notify", shim_builder)


@operator(_core=True)
def unary_notify(
    up: KeyedStream,
    step_name: str,
    builder: Callable[[Optional[Any]], UnaryNotifyLogic],
) -> KeyedStream:
    down = up._scope._new_stream("down")
    return KeyedStream(down.stream_id, down._scope)
