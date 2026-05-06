"""Time-based interval operators."""

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.operators import (
    _EMPTY,
    KeyedStream,
    S,
    StatefulBatchLogic,
    U,
    V,
    W,
    W_co,
    X,
    Y,
    _JoinState,
)
from bytewax.windowing import (
    SC,
    UTC_MAX,
    UTC_MIN,
    Clock,
    ClockLogic,
    EventClock,
    WindowMetadata,
)
from typing_extensions import TypeAlias, overload, override

LeftRight: TypeAlias = Literal["left", "right"]
"""Which side did a value come from."""


class IntervalLogic(ABC, Generic[V, W, S]):
    """Abstract class to define a {py:obj}`interval` operator.

    That operator will call these methods for you.

    A unique instance of this logic will be instantiated for each item
    seen on the left side.

    """

    @abstractmethod
    def on_value(self, side: LeftRight, value: V) -> Iterable[W]:
        """Called on each new upstream item in within this interval.

        Will be called only once with a left side item that created
        the interval.

        :arg side: Either `"left"` or `"right"`.

        :arg value: The value of the upstream `(key, value)`.

        :returns: Any values to emit downstream. Values will
            automatically be wrapped with key.

        """
        ...

    @abstractmethod
    def on_close(self) -> Iterable[W]:
        """Called when this interval closes.

        :returns: Any values to emit downstream. Values will
            automatically be wrapped with key.

        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of {py:obj}`interval` when resuming.

        The state must be {py:obj}`pickle`-able.

        :::{danger}

        **The state must be effectively immutable!** If any of the
        other functions in this class might be able to mutate the
        state, you must {py:obj}`copy.deepcopy` or something
        equivalent before returning it here.

        :::

        :returns: The immutable state to be {py:obj}`pickle`d.

        """
        ...


@dataclass
class _IntervalQueueEntry(Generic[V]):
    value: V
    timestamp: datetime
    paired: bool = False


@dataclass(frozen=True)
class _IntervalLogicSnapshot(Generic[S]):
    meta: WindowMetadata
    logic_snap: S
    max_timestamp: datetime


@dataclass
class _IntervalLogicState(Generic[V, W, S]):
    meta: WindowMetadata
    logic: IntervalLogic[V, W, S]
    max_timestamp: datetime = UTC_MIN

    def snapshot(self) -> _IntervalLogicSnapshot[S]:
        return _IntervalLogicSnapshot(
            self.meta,
            self.logic.snapshot(),
            self.max_timestamp,
        )


@dataclass(frozen=True)
class _IntervalSnapshot(Generic[V, SC, S]):
    left_clock_snap: SC
    right_clock_snap: SC
    queue: List[_IntervalQueueEntry[V]]
    logic_snaps: List[_IntervalLogicSnapshot[S]]


_Late: TypeAlias = Tuple[Literal["L"], V]
_Emit: TypeAlias = Tuple[Literal["E"], V]
_Unpaired: TypeAlias = Tuple[Literal["U"], V]
_IntervalEvent: TypeAlias = Union[_Late[V], _Emit[W], _Unpaired[V]]


@dataclass
class _IntervalLogic(
    StatefulBatchLogic[
        Tuple[LeftRight, V],
        _IntervalEvent[V, W],
        _IntervalSnapshot[V, SC, S],
    ]
):
    left_clock: ClockLogic[V, SC]
    right_clock: ClockLogic[V, SC]
    gap_before: timedelta
    gap_after: timedelta
    builder: Callable[[Optional[S]], IntervalLogic[V, W, S]]

    queue: List[_IntervalQueueEntry[V]] = field(default_factory=list)
    opened: List[_IntervalLogicState[V, W, S]] = field(default_factory=list)

    _last_left_watermark: datetime = UTC_MIN
    _last_right_watermark: datetime = UTC_MIN

    def _handle_inserted(self, watermark: datetime) -> Iterable[_IntervalEvent[V, W]]:
        last_timestamp = UTC_MIN
        # Cache the max timestamps so we can process multiple new
        # items with the same timestamp without thinking they were
        # already processed.
        last_max_timestamps = [state.max_timestamp for state in self.opened]
        for entry in self.queue:
            assert last_timestamp <= entry.timestamp, "queue is not in timestamp order"

            # Queue is in timestamp order, only operate on items for
            # which the order has been finalized.
            if entry.timestamp >= watermark:
                return
            else:
                last_timestamp = entry.timestamp

            for state, max_timestamp in zip(self.opened, last_max_timestamps):
                # If we already processed this item, skip.
                if entry.timestamp <= max_timestamp:
                    continue

                if (
                    entry.timestamp >= state.meta.open_time
                    and entry.timestamp <= state.meta.close_time
                ):
                    ws = state.logic.on_value("right", entry.value)
                    yield from (("E", w) for w in ws)

                    entry.paired = True

                    assert state.max_timestamp <= entry.timestamp
                    state.max_timestamp = entry.timestamp

    def _handle_closed(self, watermark: datetime) -> Iterable[_IntervalEvent[V, W]]:
        try:
            discard_before = watermark - self.gap_before
        except OverflowError:
            discard_before = UTC_MIN
        still_queue = []
        for entry in self.queue:
            # Keep around items before the watermark in the queue in
            # case we get a new left-side item that is right at the
            # watermark and need to look "back in time" to pair.
            if entry.timestamp >= discard_before:
                still_queue.append(entry)
            elif not entry.paired:
                yield ("U", entry.value)

        self.queue = still_queue

        still_opened = []
        for state in self.opened:
            if state.meta.close_time <= watermark:
                yield from (("E", w) for w in state.logic.on_close())
            else:
                still_opened.append(state)

        self.opened = still_opened

    def _is_empty(self) -> bool:
        return len(self.queue) <= 0 and len(self.opened) <= 0

    @override
    def on_batch(
        self, values: List[Tuple[LeftRight, V]]
    ) -> Tuple[Iterable[_IntervalEvent[V, W]], bool]:
        self.left_clock.before_batch()
        self.right_clock.before_batch()
        events: List[_IntervalEvent[V, W]] = []

        for side, value in values:
            if side == "left":
                timestamp, left_watermark = self.left_clock.on_item(value)
                assert left_watermark >= self._last_left_watermark
                self._last_left_watermark = left_watermark

                if timestamp < self._last_left_watermark:
                    events.append(("L", value))
                    continue

                try:
                    open_time = timestamp - self.gap_before
                except OverflowError:
                    open_time = UTC_MIN
                try:
                    close_time = timestamp + self.gap_after
                except OverflowError:
                    close_time = UTC_MAX
                meta = WindowMetadata(open_time, close_time)

                logic = self.builder(None)

                state = _IntervalLogicState(meta, logic)
                self.opened.append(state)

                ws = logic.on_value("left", value)
                events.extend(("E", w) for w in ws)
            elif side == "right":
                timestamp, right_watermark = self.right_clock.on_item(value)
                assert right_watermark >= self._last_right_watermark
                self._last_right_watermark = right_watermark

                if timestamp < self._last_right_watermark:
                    events.append(("L", value))
                    continue
                entry = _IntervalQueueEntry(value, timestamp)
                self.queue.append(entry)
            else:
                msg = f"unknown interval side {side!r}"
                raise ValueError(msg)

        self.queue.sort(key=lambda entry: entry.timestamp)

        watermark = min(self._last_left_watermark, self._last_right_watermark)
        events.extend(self._handle_inserted(watermark))
        events.extend(self._handle_closed(watermark))

        return (events, self._is_empty())

    @override
    def on_notify(self) -> Tuple[Iterable[_IntervalEvent[V, W]], bool]:
        left_watermark = self.left_clock.on_notify()
        assert left_watermark >= self._last_left_watermark
        self._last_left_watermark = left_watermark

        right_watermark = self.right_clock.on_notify()
        assert right_watermark >= self._last_right_watermark
        self._last_right_watermark = right_watermark

        watermark = min(left_watermark, right_watermark)

        events = list(self._handle_inserted(watermark))
        events.extend(self._handle_closed(watermark))

        return (events, self._is_empty())

    @override
    def on_eof(self) -> Tuple[Iterable[_IntervalEvent[V, W]], bool]:
        left_watermark = self.left_clock.on_eof()
        assert left_watermark >= self._last_left_watermark
        self._last_left_watermark = left_watermark

        right_watermark = self.right_clock.on_eof()
        assert right_watermark >= self._last_right_watermark
        self._last_right_watermark = right_watermark

        watermark = min(left_watermark, right_watermark)

        events = list(self._handle_inserted(watermark))
        events.extend(self._handle_closed(watermark))

        return (events, self._is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        notify_at = min(
            (state.meta.close_time for state in self.opened),
            default=None,
        )
        if notify_at is not None:
            notify_at = self.left_clock.to_system_utc(notify_at)
        return notify_at

    @override
    def snapshot(self) -> _IntervalSnapshot[V, SC, S]:
        return _IntervalSnapshot(
            self.left_clock.snapshot(),
            self.right_clock.snapshot(),
            copy.deepcopy(self.queue),
            [state.snapshot() for state in self.opened],
        )


@dataclass(frozen=True)
class IntervalOut(Generic[V, W_co]):
    """Streams returned from an interval operator."""

    down: KeyedStream[W_co]
    """Items emitted from this operator."""

    late: KeyedStream[V]
    """Upstream items that were before the current watermark."""

    unpaired: KeyedStream[V]
    """Items from right side which did not fall into any interval."""


def _unwrap_interval_emit(event: _IntervalEvent[V, W]) -> Optional[W]:
    typ, obj = event
    if typ == "E":
        value = cast(W, obj)
        return value
    else:
        return None


def _unwrap_interval_late(event: _IntervalEvent[V, W]) -> Optional[V]:
    typ, obj = event
    if typ == "L":
        value = cast(V, obj)
        return value
    else:
        return None


def _unwrap_interval_unpaired(event: _IntervalEvent[V, W]) -> Optional[V]:
    typ, obj = event
    if typ == "U":
        value = cast(V, obj)
        return value
    else:
        return None


@operator
def interval(
    step_id: str,
    left: KeyedStream[V],
    clock: Clock[V, SC],
    gap_before: timedelta,
    gap_after: timedelta,
    builder: Callable[[Optional[S]], IntervalLogic[V, W, S]],
    right: KeyedStream[V],
) -> IntervalOut[V, W]:
    """Low level interval operator.

    This allows opening "windows" whenever an item occurs on the left
    side, and then pairs it with items in the right side that are
    within the specified timestamp gap.

    Values are always applied to the logic in timestamp (not arrival)
    order.

    :arg step_id: Unique ID.

    :arg left: Triggering stream.

    :arg clock: Time definition. Must be able to determine timestamps
        for both `left` and `right` streams.

    :arg gap_before: Pair right side items if they are within this
        duration before a left side item.

    :arg gap_after: Pair right side items if they are within this
        duration after a left side item.

    :arg builder: Builds an {py:obj}`IntervalLogic` whenever a new
        left side value is seen, or state is being reconstituted
        during a resume.

    :arg right: Joined stream.

    :returns: Interval result streams.

    """
    named_left = op.map_value("name_left", left, lambda v: ("left", v))
    named_right = op.map_value("name_right", right, lambda v: ("right", v))
    # mypy doesn't know when to convert a `str` to a `Literal`.
    merged = cast(
        KeyedStream[Tuple[LeftRight, V]], op.merge("merge", named_left, named_right)
    )

    def shim_logic_builder(
        resume_state: _IntervalLogicSnapshot[S],
    ) -> _IntervalLogicState[V, W, S]:
        logic = builder(resume_state.logic_snap)
        return _IntervalLogicState(resume_state.meta, logic, resume_state.max_timestamp)

    def shim_builder(
        resume_state: Optional[_IntervalSnapshot[V, SC, S]],
    ) -> _IntervalLogic[V, W, SC, S]:
        if resume_state is not None:
            left_clock_logic = clock.build(resume_state.left_clock_snap)
            right_clock_logic = clock.build(resume_state.right_clock_snap)
            logics = [shim_logic_builder(snap) for snap in resume_state.logic_snaps]
            return _IntervalLogic(
                left_clock_logic,
                right_clock_logic,
                gap_before,
                gap_after,
                builder,
                resume_state.queue,
                logics,
            )
        else:
            left_clock_logic = clock.build(None)
            right_clock_logic = clock.build(None)
            return _IntervalLogic(
                left_clock_logic,
                right_clock_logic,
                gap_before,
                gap_after,
                builder,
            )

    events = op.stateful_batch("stateful_batch", merged, shim_builder)
    downs: KeyedStream[W] = op.filter_map_value(
        "unwrap_down",
        events,
        _unwrap_interval_emit,
    )
    lates: KeyedStream[V] = op.filter_map_value(
        "unwrap_late",
        events,
        _unwrap_interval_late,
    )
    unpaireds: KeyedStream[V] = op.filter_map_value(
        "unwrap_unpaired",
        events,
        _unwrap_interval_unpaired,
    )
    return IntervalOut(downs, lates, unpaireds)


@dataclass
class _JoinIntervalCompleteLogic(IntervalLogic[Tuple[int, V], _JoinState, _JoinState]):
    state: _JoinState

    @override
    def on_value(self, side: LeftRight, value: Tuple[int, V]) -> Iterable[_JoinState]:
        join_side, join_value = value
        self.state.set_val(join_side, join_value)

        if self.state.all_set():
            state = copy.deepcopy(self.state)
            # Only reset all right sides since we'll never see left side
            # again by definition in an interval.
            for i in range(1, len(self.state.seen)):
                self.state.seen[i] = []
            return (state,)
        else:
            return _EMPTY

    @override
    def on_close(self) -> Iterable[_JoinState]:
        return _EMPTY

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


@dataclass
class _JoinIntervalFinalLogic(IntervalLogic[Tuple[int, V], _JoinState, _JoinState]):
    state: _JoinState

    @override
    def on_value(self, side: LeftRight, value: Tuple[int, V]) -> Iterable[_JoinState]:
        join_side, join_value = value
        self.state.set_val(join_side, join_value)
        return _EMPTY

    @override
    def on_close(self) -> Iterable[_JoinState]:
        # No need to deepcopy because we are discarding the state.
        return (self.state,)

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


@dataclass
class _JoinIntervalRunningLogic(IntervalLogic[Tuple[int, V], _JoinState, _JoinState]):
    state: _JoinState

    @override
    def on_value(self, side: LeftRight, value: Tuple[int, V]) -> Iterable[_JoinState]:
        join_side, join_value = value
        self.state.set_val(join_side, join_value)
        return (copy.deepcopy(self.state),)

    @override
    def on_close(self) -> Iterable[_JoinState]:
        return _EMPTY

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


@dataclass
class _JoinIntervalProductLogic(IntervalLogic[Tuple[int, V], _JoinState, _JoinState]):
    state: _JoinState

    @override
    def on_value(self, side: LeftRight, value: Tuple[int, V]) -> Iterable[_JoinState]:
        join_side, join_value = value
        self.state.add_val(join_side, join_value)
        return _EMPTY

    @override
    def on_close(self) -> Iterable[_JoinState]:
        # No need to deepcopy because we are discarding the state.
        return (self.state,)

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


def _add_side_builder(i: int) -> Callable[[V], Tuple[int, V]]:
    def add_side(v: V) -> Tuple[int, V]:
        return (i, v)

    return add_side


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[U],
    clock: Clock[Union[U, V], Any],
    gap_before: timedelta,
    gap_after: timedelta,
    right1: KeyedStream[V],
    /,
    *,
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[V, Tuple[U, V]]: ...


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[U],
    clock: Clock[Union[U, V, W], Any],
    gap_before: timedelta,
    gap_after: timedelta,
    right1: KeyedStream[V],
    right2: KeyedStream[W],
    /,
    *,
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[Union[V, W], Tuple[U, V, W]]: ...


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[U],
    clock: Clock[Union[U, V, W, X], Any],
    gap_before: timedelta,
    gap_after: timedelta,
    right1: KeyedStream[V],
    right2: KeyedStream[W],
    right3: KeyedStream[X],
    /,
    *,
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[Union[V, W, X], Tuple[U, V, W, X]]: ...


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[U],
    clock: Clock[Union[U, V, W, X, Y], Any],
    gap_before: timedelta,
    gap_after: timedelta,
    right1: KeyedStream[V],
    right2: KeyedStream[W],
    right3: KeyedStream[X],
    right4: KeyedStream[Y],
    /,
    *,
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[Union[V, W, X, Y], Tuple[U, V, W, X, Y]]: ...


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[V],
    clock: Clock[V, Any],
    gap_before: timedelta,
    gap_after: timedelta,
    *rights: KeyedStream[V],
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[V, Tuple[V, ...]]: ...


@overload
def join_interval(
    step_id: str,
    left: KeyedStream[Any],
    clock: Clock[Any, Any],
    gap_before: timedelta,
    gap_after: timedelta,
    *rights: KeyedStream[Any],
    mode: Literal["complete", "final", "running", "product"] = ...,
) -> IntervalOut[Any, Tuple[Any, ...]]: ...


@operator
def join_interval(
    step_id: str,
    left: KeyedStream[Any],
    clock: Clock[Any, Any],
    gap_before: timedelta,
    gap_after: timedelta,
    *rights: KeyedStream[Any],
    mode: Literal["complete", "final", "running", "product"] = "final",
) -> IntervalOut[Any, Tuple]:
    """Gather values for a key which lie near each other in time.

    The joining process is triggered only by left side values.

    :arg step_id: Unique ID.

    :arg left: Triggering stream.

    :arg clock: Time definition. Must be able to determine timestamps
        for both `left` and `rights` streams.

    :arg gap_before: Join right side items if they are within this
        duration before a left side item.

    :arg gap_before: Join right side items if they are within this
        duration after a left side item.

    :arg rights: Joined streams.

    :arg mode: What kind of join to do. Determines the output if there
        is not just a single value on each side within the interval.
        One of:

        - `"final"`: Emit a single tuple with the last value (by
          timestamp) on each side. A side will contain `None` if no
          value seen.

        - `"running"`: Emit a tuple of the most recent values for all
          sides each time there is a new value on any side. The most
          recent value for a side starts as `None`.

        - `"product"`: Emit a tuple for each combination of values on
          each side. A side will contain `None` if no value seen.

        Defaults to `"final"`.

    :returns: Interval result streams. Downstream contains tuples with
        the value from each stream in the order of the argument list
        once each interval has closed.

    """
    sided_left = op.map_value("side_left_0", left, _add_side_builder(0))
    sided_rights = [
        op.map_value(f"side_right_{i + 1}", right, _add_side_builder(i + 1))
        for i, right in enumerate(rights)
    ]
    merged_rights = op.merge("merge", *sided_rights)

    # TODO: Egregious hack. Remove when we refactor to have timestamps
    # in stream.
    if isinstance(clock, EventClock):
        value_ts_getter = clock.ts_getter

        def shim_getter(i_v: Tuple[str, V]) -> datetime:
            _, v = i_v
            return value_ts_getter(v)

        clock = EventClock(
            ts_getter=shim_getter,
            wait_for_system_duration=clock.wait_for_system_duration,
            now_getter=clock.now_getter,
            to_system_utc=clock.to_system_utc,
        )

    logic_class: Callable[
        [_JoinState], IntervalLogic[Tuple[int, V], _JoinState, _JoinState]
    ]
    if mode == "complete":
        logic_class = _JoinIntervalCompleteLogic
    elif mode == "final":
        logic_class = _JoinIntervalFinalLogic
    elif mode == "running":
        logic_class = _JoinIntervalRunningLogic
    elif mode == "product":
        logic_class = _JoinIntervalProductLogic
    else:
        msg = f"unknown `join_interval` mode: {mode!r}"
        raise ValueError(msg)

    def shim_builder(
        resume_state: Optional[_JoinState],
    ) -> IntervalLogic[Tuple[int, V], _JoinState, _JoinState]:
        state = _JoinState.for_side_count(len(rights) + 1)
        return logic_class(state)

    interval_out = interval(
        "interval",
        sided_left,
        clock,
        gap_before,
        gap_after,
        shim_builder,
        merged_rights,
    )
    return IntervalOut(
        op.flat_map_value("astuple", interval_out.down, _JoinState.astuples),
        interval_out.late,
        interval_out.unpaired,
    )
