"""Stateful operators that have timeout parameters."""

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Generic, Iterable, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.operators import (
    _EMPTY,
    KeyedStream,
    S,
    StatefulLogic,
    V,
    W,
    _get_system_utc,
    f_repr,
)
from typing_extensions import override

ZERO_TD: timedelta = timedelta(seconds=0)
"""A zero length of time."""


@dataclass
class _StatefulFlatMapTimeoutState(Generic[S]):
    state: Optional[S] = None
    last_value_at: Optional[datetime] = None


@dataclass
class _StatefulFlatMapTimeoutLogic(
    StatefulLogic[V, W, _StatefulFlatMapTimeoutState[S]]
):
    step_id: str
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], Iterable[W]]]
    timeout: timedelta
    on_timeout: Callable[[S], Iterable[W]]
    now_getter: Callable[[], datetime]

    state: _StatefulFlatMapTimeoutState[S] = field(
        default_factory=lambda: _StatefulFlatMapTimeoutState()
    )

    @override
    def on_item(self, value: V) -> Tuple[Iterable[W], bool]:
        # I don't think it makes sense to refactor this to be based on
        # `stateful_batch` for performance reasons because you still
        # need to call `now_getter` once per key and it's not possible
        # to correctly have a "before all keys in a batch" callback
        # because there's no single logic that represents that.
        self.state.last_value_at = self.now_getter()
        res = self.mapper(self.state.state, value)
        try:
            s, ws = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(self.mapper)} "
                f"in step {self.step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_values)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex

        if s is None:
            is_complete = StatefulLogic.DISCARD
        else:
            is_complete = StatefulLogic.RETAIN

        self.state.state = s
        return (ws, is_complete)

    @override
    def on_notify(self) -> Tuple[Iterable[W], bool]:
        assert self.state.state is not None
        ws = self.on_timeout(self.state.state)
        return (ws, StatefulLogic.DISCARD)

    @override
    def on_eof(self) -> Tuple[Iterable[W], bool]:
        assert self.state.state is not None
        ws = self.on_timeout(self.state.state)
        return (ws, StatefulLogic.DISCARD)

    @override
    def notify_at(self) -> Optional[datetime]:
        assert self.state.last_value_at is not None
        try:
            return self.state.last_value_at + self.timeout
        except OverflowError:
            # If the expiration time is unrepresentable because it's
            # so far in the future, that's the same as never expiring.
            return None

    @override
    def snapshot(self) -> _StatefulFlatMapTimeoutState[S]:
        return copy.deepcopy(self.state)


@operator
def stateful_flat_map_timeout(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], Iterable[W]]],
    timeout: timedelta,
    on_timeout: Callable[[S], Iterable[W]] = lambda s: _EMPTY,
    _now_getter: Callable[[], datetime] = _get_system_utc,
) -> KeyedStream[W]:
    """One-to-many transform using an expiring persistent state.

    Works similar to {py:obj}`~bytewax.operators.stateful_flat_map`,
    but will discard the state if no value is seen for a key after a
    timeout. Just before a discard due to timeout, `on_timeout` is
    called and allows you to emit items downstream referencing just
    the state.

    All keys' state times out on EOF and `on_timeout` is called.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called whenever a value is encountered from upstream
        with the last state or `None`, and then the upstream value.
        Should return a 2-tuple of `(updated_state, emit_values)`. If
        the updated state is `None`, discard it.

    :arg timeout: Discard state after this duration if a value isn't
        seen again for a key.

    :arg on_timeout: Called just before discarding the state for a key
        if due to timeout. Should return any additional values to emit
        downstream just before the state is discarded. Defaults to
        emitting nothing downstream on expiration.

    :returns: A keyed stream.

    """

    def shim_builder(
        resume_state: Optional[_StatefulFlatMapTimeoutState[S]],
    ) -> _StatefulFlatMapTimeoutLogic[V, W, S]:
        if resume_state is not None:
            return _StatefulFlatMapTimeoutLogic(
                step_id,
                mapper,
                timeout,
                on_timeout,
                _now_getter,
                resume_state,
            )
        else:
            return _StatefulFlatMapTimeoutLogic(
                step_id,
                mapper,
                timeout,
                on_timeout,
                _now_getter,
            )

    return op.stateful("stateful", up, shim_builder)


@operator
def stateful_map_timeout(
    step_id: str,
    up: KeyedStream[V],
    mapper: Callable[[Optional[S], V], Tuple[Optional[S], W]],
    timeout: timedelta,
    _now_getter: Callable[[], datetime] = _get_system_utc,
) -> KeyedStream[W]:
    """One-to-one transform using an expiring persistent state.

    Works exactly like {py:obj}`~bytewax.operators.stateful_map`, but
    will discard the state if no value is seen for a key after a
    timeout. Nothing is emitted downstream on expiration.

    All keys' state times out on EOF and `on_timeout` is called.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg mapper: Called whenever a value is encountered from upstream
        with the last state or `None`, and then the upstream value.
        Should return a 2-tuple of `(updated_state, emit_value)`. If
        the updated state is `None`, discard it.

    :arg timeout: Discard state after this duration if a value isn't
        seen again for a key.

    :returns: A keyed stream.

    """

    def shim_mapper(state: Optional[S], v: V) -> Tuple[Optional[S], Iterable[W]]:
        res = mapper(state, v)
        try:
            s, w = res
        except TypeError as ex:
            msg = (
                f"return value of `mapper` {f_repr(mapper)} "
                f"in step {step_id!r} "
                "must be a 2-tuple of `(updated_state, emit_value)`; "
                f"got a {type(res)!r} instead"
            )
            raise TypeError(msg) from ex

        return (s, (w,))

    return stateful_flat_map_timeout(
        "stateful_flat_map_timeout",
        up,
        shim_mapper,
        timeout,
        _now_getter=_now_getter,
    )
