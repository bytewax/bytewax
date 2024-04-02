"""Time-based windowing operators."""

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import Self, TypeAlias, override

import bytewax.operators as op
from bytewax.dataflow import (
    Stream,
    operator,
)
from bytewax.operators import (
    _EMPTY,
    KeyedStream,
    S,
    StatefulBatchLogic,
    V,
    W,
    X,
    _identity,
    _JoinState,
    _untyped_none,
)

ZERO_TD = timedelta(seconds=0)
"""A zero length of time."""


UTC_MAX = datetime.max.replace(tzinfo=timezone.utc)
"""Maximum possible UTC date time."""


UTC_MIN = datetime.min.replace(tzinfo=timezone.utc)
"""Minimum possible UTC date time."""


SC = TypeVar("SC")
"""Type of {py:obj}`ClockLogic`'s state snapshots."""


SW = TypeVar("SW")
"""Type of {py:obj}`WindowerLogic`'s state snapshots."""


C = TypeVar("C", bound=Iterable)
"""Type of downstream containers."""


DK = TypeVar("DK")
"""Type of {py:obj}`dict` keys."""


DV = TypeVar("DV")
"""Type of {py:obj}`dict` values."""


class ClockLogic(ABC, Generic[V, S]):
    """Abstract class to define a sense of time for windowing.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Clock` for built-in options.

    This type is instantiated within Bytewax's windowing operators and
    has methods called by the runtime. A new subclass of this will
    need to be paired with a new subclass of {py:obj}`Clock`, which is
    holds the configuration data for this type.

    This is instantiated for each key which is encountered.

    """

    @abstractmethod
    def on_batch(self) -> None:
        """Prepare to process items incoming simultaneously.

        Called once before a series of {py:obj}`on_item` calls.

        You can use this to cache a "current time" or prepare other
        state.

        """
        ...

    @abstractmethod
    def on_item(self, value: V) -> Tuple[datetime, datetime]:
        """Get the timestamp and watermark after this item.

        Called on each new upstream item.

        If new timestamps will permanently affect the watermark,
        relevant state must be retained within.

        :arg value: The value of the upstream `(key, value)`.

        :returns: A 2-tuple of timestamp for the item and the current
            watermark.

        """
        ...

    @abstractmethod
    def on_notify(self) -> datetime:
        """Get the current watermark when there is no item.

        Called when a window might need to close.

        :returns: The current watermark.

        """
        ...

    @abstractmethod
    def on_eof(self) -> datetime:
        """Get the current watermark when no more items upstream.

        Generally you'd return {py:obj}`UTC_MAX` if you want to close
        all windows just before stopping because the window semantics
        don't make sense across a resume.

        To support continuation resumes, this should not permanently
        affect the state that is snapshot.

        :returns: The current watermark.

        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to
        {py:obj}`Clock.build` when resuming.

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
class _SystemClockLogic(ClockLogic[V, None]):
    _now: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._now = datetime.now(tz=timezone.utc)

    @override
    def on_batch(self) -> None:
        self._now = datetime.now(tz=timezone.utc)

    @override
    def on_item(self, value: V) -> Tuple[datetime, datetime]:
        return (self._now, self._now)

    @override
    def on_notify(self) -> datetime:
        self._now = datetime.now(tz=timezone.utc)
        return self._now

    @override
    def on_eof(self) -> datetime:
        self._now = datetime.now(tz=timezone.utc)
        return self._now

    @override
    def snapshot(self) -> None:
        return None


@dataclass
class _EventClockState:
    max_event_timestamp: datetime
    system_time_of_max_event: datetime
    watermark_base: datetime


@dataclass
class _EventClockLogic(ClockLogic[V, Optional[_EventClockState]]):
    now_getter: Callable[[], datetime]
    timestamp_getter: Callable[[V], datetime]
    wait_for_system_duration: timedelta
    state: Optional[_EventClockState]
    _system_now: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._system_now = self.now_getter()

    @override
    def on_batch(self) -> None:
        self._system_now = self.now_getter()

    @override
    def on_item(self, value: V) -> Tuple[datetime, datetime]:
        value_event_timestamp = self.timestamp_getter(value)

        if self.state is None:
            self.state = _EventClockState(
                max_event_timestamp=value_event_timestamp,
                system_time_of_max_event=self._system_now,
                watermark_base=value_event_timestamp - self.wait_for_system_duration,
            )
        elif value_event_timestamp > self.state.max_event_timestamp:
            self.state.max_event_timestamp = value_event_timestamp
            self.state.system_time_of_max_event = self._system_now
            self.state.watermark_base = (
                value_event_timestamp - self.wait_for_system_duration
            )

        return value_event_timestamp, self.state.watermark_base

    @override
    def on_notify(self) -> datetime:
        if self.state is None:
            return UTC_MIN
        else:
            self._system_now = self.now_getter()
            return self.state.watermark_base + (
                self._system_now - self.state.system_time_of_max_event
            )

    @override
    def on_eof(self) -> datetime:
        return UTC_MAX

    @override
    def snapshot(self) -> Optional[_EventClockState]:
        return copy.deepcopy(self.state)


class Clock(ABC, Generic[V, S]):
    """Abstract class defining a type of clock.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Clock` for built-in options.

    Every subclass of this class must also have a matching
    implementation of {py:obj}`ClockLogic` that actually implements
    the clock.

    """

    @abstractmethod
    def build(self, resume_state: Optional[S]) -> ClockLogic[V, S]:
        """Construct a new clock logic instance.

        This should close over any non-state configuration and combine
        it with the resume state to return a prepared
        {py:obj}`ClockLogic` instance.

        :arg resume_state: Snapshot from the previous call to
            {py:obj}`ClockLogic.snapshot`, if any.

        :returns: A prepared clock logic.

        """
        ...


@dataclass
class SystemClock(Clock[V, None]):
    """Uses the current system time as the timestamp for each item.

    The watermark is the current system time.

    All timestamps and watermarks are generated in UTC.

    When the dataflow has no more input, all windows are closed.

    """

    @override
    def build(self, resume_state: None) -> _SystemClockLogic[V]:
        return _SystemClockLogic()


def _get_system_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass
class EventClock(Clock[V, Optional[_EventClockState]]):
    """Use a timestamp embedded within each item.

    The watermark is the largest timestamp seen thus far, minus the
    waiting duration, plus the system time duration that has elapsed
    since that item was seen. This effectively means that items will
    be correctly processed as long as they do not have timestamps
    out-of-order or arrive later than the waiting duration.

    :::{note}

    Because `now_getter` defaults to returning the current system time
    as [aware datetimes](inv:python:term#aware-and-naive-objects) in
    UTC, all timestamps returned by `ts_getter` also need to be [aware
    datetimes](inv:python:term#aware-and-naive-objects) in UTC.

    It's easiest to convert your timestamps to UTC, but you can also
    override `now_getter` to support other sense of time.

    :::

    :arg ts_getter: Called once on each item to get its timestamp.

    :arg wait_for_system_duration: How much system time to wait after
        seeing a timestamp to have the watermark catch up to that time.

    :arg now_getter: Return the current "system" timestamp used to
        advance the watermark. Defaults to the current system time in
        UTC.

    """

    ts_getter: Callable[[V], datetime]
    wait_for_system_duration: timedelta
    now_getter: Callable[[], datetime] = _get_system_utc

    @override
    def build(self, resume_state: Optional[_EventClockState]) -> _EventClockLogic[V]:
        return _EventClockLogic(
            self.now_getter, self.ts_getter, self.wait_for_system_duration, resume_state
        )


@dataclass
class WindowMetadata:
    """Metadata about a window.

    The exact semantics of the fields will depend on the windower
    used. E.g. for {py:obj}`SessionWindower`, `close_time` is
    inclusive; but for {py:obj}`SlidingWindower` it is not.

    """

    open_time: datetime
    """The timestamp this window opened."""
    close_time: datetime
    """The timestamp this window closed."""


class WindowerLogic(ABC, Generic[S]):
    """Abstract class which defines a type of window.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Windower` for built-in options.

    This type is instantiated within Bytewax's windowing operators and
    has methods called by the runtime. A new subclass of this will
    need to be paired with a new subclass of {py:obj}`Windower`, which
    is holds the configuration data for this type.

    This is instantiated for each key which is encountered.

    """

    @abstractmethod
    def open_for(
        self,
        timestamp: datetime,
        watermark: datetime,
    ) -> Tuple[Iterable[int], Iterable[int]]:
        """Find which windows an item is in and mark them as open.

        Called when a new item arrives.

        You'll need to do whatever bookkeeping is necessary internally
        to be able to satisfy the other abstract methods, like being
        able to return the metadata for a window.

        :arg timestamp: Of the incoming item.

        :arg watermark: Current watermark. Guaranteed to never
            regress across calls.

        :returns: A 2-tuple of a list of window IDs that this item is
            in, and a list of window IDs this item is late for.

        """
        ...

    @abstractmethod
    def merged(self) -> Iterable[Tuple[int, int]]:
        """Report any windows that have been merged.

        A merge is reported by saying an **original window** should be
        merged into a **target window**.

        This will be called after a batch of items has been processed.
        The system assumes merges can only occur because of incoming
        items.

        You only need to report a merge once. After a window ID has
        been returned as an original window, that ID will not be
        passed to {py:obj}`~WindowerLogic.metadata_for` nor does it
        need to be reported as closed.

        :returns: An iterable of 2-tuples of `(original_window_id,
            target_window_id)`.

        """
        ...

    @abstractmethod
    def metadata_for(self, window_id: int) -> WindowMetadata:
        """Return metadata about an open window.

        Called when encountering a new window or there was an
        indication the window boundaries may have changed due to a
        merge.

        :arg window_id: Open window ID. Will never be a window that
            has been closed.

        :returns: Metadata for this window.

        """
        ...

    @abstractmethod
    def close_for(self, watermark: datetime) -> Iterable[int]:
        """Report what windows are now closed.

        Called periodically once the watermark has advanced.

        Once a window ID is returned from this method, it should never
        be opened again (although items may be reported as late for
        it). Internally, you can discard whatever state was being
        maintained about this window.

        :arg watermark: Current watermark. Guaranteed to never
            regress across calls.


        :returns: A list of now closed window IDs.

        """
        ...

    @abstractmethod
    def notify_at(self) -> Optional[datetime]:
        """Next system time timestamp for which a window might close.

        This will be called periodically to know when to wake up this
        operator to close windows, even if no items were recevived.

        :returns: System time of when to next wake up.

        """
        ...

    @abstractmethod
    def is_empty(self) -> bool:
        """If no state needs to be maintained anymore.

        This, in general, is if there are no windows currently open.
        But you might have other state that needs to persist across
        resumes, and so might return `True` here to maintain that.

        If `False` is returned here, the entire windower will be
        discarded as to not "leak" memory. A new windower will be
        built if this state key is encountered again.

        :returns: If no state needs to be maintained.

        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to
        {py:obj}`Windower.build` when resuming.

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
class _SlidingWindowerState:
    opened: Dict[int, WindowMetadata] = field(default_factory=dict)


@dataclass
class _SlidingWindowerLogic(WindowerLogic[_SlidingWindowerState]):
    length: timedelta
    offset: timedelta
    align_to: datetime
    _origin_close: datetime = field(init=False)
    _overlap_factor: int = field(init=False)
    _overlap_remainder: timedelta = field(init=False)
    state: _SlidingWindowerState

    def __post_init__(self):
        # Don't support gaps. Nice error thrown in `SlidingWindower`.
        assert self.offset <= self.length
        # Pre-calc constants for performance.
        self._origin_close = self.align_to + self.length
        # Hell yes: "upside-down floor division" to get ceil.
        self._overlap_factor = -(self.length // -self.offset)
        self._overlap_remainder = self.length % self.offset

    def intersects(self, timestamp: datetime) -> List[int]:
        since_close_origin_window = timestamp - self._origin_close
        first_window_idx = since_close_origin_window // self.offset + 1
        if self._overlap_remainder == ZERO_TD:
            factor_reduction = 0
        else:
            close_remainder = since_close_origin_window % self.offset
            factor_reduction = 0 if close_remainder > self._overlap_remainder else 1
        return [
            first_window_idx + i for i in range(self._overlap_factor - factor_reduction)
        ]

    def _metadata_for(self, window_id: int) -> WindowMetadata:
        open_time = self.align_to + self.offset * window_id
        close_time = open_time + self.length
        return WindowMetadata(open_time, close_time)

    @override
    def open_for(
        self,
        timestamp: datetime,
        watermark: datetime,
    ) -> Tuple[List[int], List[int]]:
        in_windows = []
        late_windows = []
        for window_id in self.intersects(timestamp):
            meta = self.state.opened.get(window_id) or self._metadata_for(window_id)
            if meta.close_time <= watermark:
                late_windows.append(window_id)
            else:
                self.state.opened.setdefault(window_id, meta)
                in_windows.append(window_id)

        return (in_windows, late_windows)

    @override
    def merged(self) -> Iterable[Tuple[int, int]]:
        """Sliding windows never merge."""
        return _EMPTY

    @override
    def metadata_for(self, window_id: int) -> WindowMetadata:
        return self.state.opened[window_id]

    @override
    def close_for(self, watermark: datetime) -> Iterable[int]:
        closed = [
            window_id
            for window_id, meta in self.state.opened.items()
            if meta.close_time <= watermark
        ]
        for window_id in closed:
            del self.state.opened[window_id]

        return closed

    @override
    def notify_at(self) -> Optional[datetime]:
        return min(
            (meta.close_time for meta in self.state.opened.values()), default=None
        )

    @override
    def is_empty(self) -> bool:
        return len(self.state.opened) <= 0

    @override
    def snapshot(self) -> _SlidingWindowerState:
        return copy.deepcopy(self.state)


LATE_SESSION_ID: int = -1


@dataclass
class _SessionWindowerState:
    max_key: int = LATE_SESSION_ID
    sessions: Dict[int, WindowMetadata] = field(default_factory=dict)
    merge_queue: List[Tuple[int, int]] = field(default_factory=list)
    """Original ID to new ID."""


def _open_sort(id_meta: Tuple[int, WindowMetadata]) -> datetime:
    _window_id, meta = id_meta
    return meta.open_time


@dataclass
class _SessionWindowerLogic(WindowerLogic[_SessionWindowerState]):
    gap: timedelta
    state: _SessionWindowerState

    def _find_merges(self) -> None:
        if len(self.state.sessions) >= 2:
            sorted_id_sessions = sorted(self.state.sessions.items(), key=_open_sort)
            merged_id_sessions = [sorted_id_sessions.pop(0)]
            for this_id, this_meta in sorted_id_sessions:
                last_window_id, last_meta = merged_id_sessions[-1]
                # See if this next window is within the gap from the
                # last close. Since we sorted by open time, this is
                # always the correct direction.
                if this_meta.open_time - last_meta.close_time <= self.gap:
                    # If so, it's a merge. Extend the close time (if
                    # it actually expands it; the merged window might
                    # be fully within the last window).
                    last_meta.close_time = max(
                        last_meta.close_time, this_meta.close_time
                    )
                    self.state.merge_queue.append((this_id, last_window_id))
                else:
                    # If they don't merge, this current window is now
                    # the new last window to attempt to merge with in
                    # the next iteration.
                    merged_id_sessions.append((this_id, this_meta))

            # Now save the new sessions.
            self.state.sessions = dict(merged_id_sessions)

    @override
    def open_for(
        self,
        timestamp: datetime,
        watermark: datetime,
    ) -> Tuple[Iterable[int], Iterable[int]]:
        if timestamp < watermark:
            return (_EMPTY, (LATE_SESSION_ID,))

        for window_id, meta in self.state.sessions.items():
            assert meta.open_time <= meta.close_time
            until_open = meta.open_time - timestamp
            since_close = timestamp - meta.close_time
            if until_open <= ZERO_TD and since_close <= ZERO_TD:
                # If we're perfectly within an existing session,
                # re-use that session. No merges possible because no
                # session boundaries changed.
                return ((window_id,), _EMPTY)
            elif until_open > ZERO_TD and until_open < self.gap:
                # If we're within the gap before an existing session,
                # re-use the session ID, but also see if any merges
                # occured.
                meta.open_time = timestamp
                self._find_merges()
                return ((window_id,), _EMPTY)
            elif since_close > ZERO_TD and since_close < self.gap:
                # Same if after an existing session.
                meta.close_time = timestamp
                self._find_merges()
                return ((window_id,), _EMPTY)

        # If we're outside all existing sessions, make anew. No need
        # to merge because we aren't within the gap of any existing
        # session.
        self.state.max_key += 1
        window_id = self.state.max_key
        self.state.sessions[window_id] = WindowMetadata(timestamp, timestamp)
        # Items are never late for a session window.
        return ((window_id,), _EMPTY)

    @override
    def merged(self) -> Iterable[Tuple[int, int]]:
        merged = self.state.merge_queue
        self.state.merge_queue = []
        return merged

    @override
    def metadata_for(self, window_id: int) -> WindowMetadata:
        return self.state.sessions[window_id]

    @override
    def close_for(self, watermark: datetime) -> Iterable[int]:
        close_after = watermark - self.gap
        closed = [
            window_id
            for window_id, meta in self.state.sessions.items()
            if meta.close_time < close_after
        ]
        for window_id in closed:
            del self.state.sessions[window_id]

        return closed

    @override
    def notify_at(self) -> Optional[datetime]:
        min_close = min(
            (meta.close_time for meta in self.state.sessions.values()),
            default=None,
        )
        return min_close + self.gap if min_close is not None else None

    @override
    def is_empty(self) -> bool:
        """Never discard state.

        If we re-used a window ID, it's possible a downstream join
        would include incorrect window metadata.

        """
        return False

    @override
    def snapshot(self) -> _SessionWindowerState:
        return copy.deepcopy(self.state)


class Windower(ABC, Generic[S]):
    """A type of window.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Windower` for built-in options.

    Every subclass of this class must also have a matching
    implementation of {py:obj}`WindowerLogic` that actually implements
    the window.

    """

    @abstractmethod
    def build(self, resume_state: Optional[S]) -> WindowerLogic[S]:
        """Construct a new windower logic instance.

        This should close over any non-state configuration and combine
        it with the resume state to return a prepared
        {py:obj}`WindowerLogic` instance.

        :arg resume_state: Snapshot from the previous call to
            {py:obj}`WindowerLogic.snapshot`, if any.

        :returns: A prepared windower logic.

        """
        ...


@dataclass
class SlidingWindower(Windower[_SlidingWindowerState]):
    """Sliding windows of fixed duration.

    If `offset == length`, windows cover all time but do not overlap.
    Each item will fall in exactly one window. This is equivalent to
    using {py:obj}`TumblingWindower`.

    If `offset < length`, windows overlap. Items could call in
    multiple windows.

    You are not allowed to set `offset > length`, otherwise there
    would be undefined gaps between windows which would lose items.

    Window open times are inclusive, but close times are exclusive.

    :arg length: Length of windows.

    :arg offset: Duration between start times of adjacent windows.

    :arg align_to: Align windows so this instant starts a window. This
        must be a constant. You can use this to align all windows to
        an hour boundary, e.g.

    """

    length: timedelta
    offset: timedelta
    align_to: datetime

    def __post_init__(self):
        if self.offset > self.length:
            msg = (
                "sliding window `offset` can't be longer than `length`; "
                "there would be undefined gaps between windows"
            )
            raise ValueError(msg)

    @override
    def build(
        self, resume_state: Optional[_SlidingWindowerState]
    ) -> _SlidingWindowerLogic:
        state = resume_state if resume_state is not None else _SlidingWindowerState()
        return _SlidingWindowerLogic(self.length, self.offset, self.align_to, state)


@dataclass
class TumblingWindower(Windower[_SlidingWindowerState]):
    """Tumbling windows of fixed duration.

    Each item will fall in exactly one window.

    Window open times are inclusive, but close times are exclusive.

    :arg length: Length of windows.

    :arg align_to: Align windows so this instant starts a window. This
        must be a constant. You can use this to align all windows to
        an hour boundary, e.g.

    """

    length: timedelta
    align_to: datetime

    @override
    def build(
        self, resume_state: Optional[_SlidingWindowerState]
    ) -> _SlidingWindowerLogic:
        state = resume_state if resume_state is not None else _SlidingWindowerState()
        return _SlidingWindowerLogic(self.length, self.length, self.align_to, state)


@dataclass
class SessionWindower(Windower[_SessionWindowerState]):
    """Session windows with a fixed inactivity gap.

    :arg gap: Gap of inactivity before considering a session closed.
        The gap must not be negative.

    """

    gap: timedelta

    def __post_init__(self):
        if self.gap < ZERO_TD:
            msg = "session window `gap` must be positive"
            raise ValueError(msg)

    @override
    def build(
        self, resume_state: Optional[_SessionWindowerState]
    ) -> _SessionWindowerLogic:
        state = resume_state if resume_state is not None else _SessionWindowerState()
        return _SessionWindowerLogic(self.gap, state)


@dataclass
class WindowLogic(ABC, Generic[V, W, S]):
    """Abstract class to define a {py:obj}`window` operator.

    That operator will call these methods for you.

    A unique instance of this logic will be instantiated for each
    unique window within each unique key.

    """

    @abstractmethod
    def on_value(self, value: V) -> Iterable[W]:
        """Called on each new upstream item within this window.

        :arg value: The value of the upstream `(key, value)`.

        :returns: Any values to emit downstream. Values will
            automatically be wrapped with key and window ID.

        """
        ...

    @abstractmethod
    def on_merge(self, original: Self) -> Iterable[W]:
        """Called when two windows should merge.

        Not all windowers result in merges.

        The `original` window state will be deleted after this.

        :arg original: Other logic to consume the state within.

        :returns: Any values to emit downstream. Values will
            automatically be wrapped with key and window ID.

        """
        ...

    @abstractmethod
    def on_close(self) -> Iterable[W]:
        """Called when this window closes.

        :returns: Any values to emit downstream. Values will
            automatically be wrapped with key and window ID.
        """
        ...

    @abstractmethod
    def snapshot(self) -> S:
        """Return a immutable copy of the state for recovery.

        This will be called periodically by the runtime.

        The value returned here will be passed back to the `builder`
        function of {py:obj}`window` when resuming.

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


@dataclass(frozen=True)
class _WindowSnapshot(Generic[SC, SW, S]):
    clock_state: SC
    windower_state: SW
    logic_states: Dict[int, S]


@dataclass(frozen=True)
class _Emit(Generic[V]):
    value: V


@dataclass(frozen=True)
class _Late(Generic[V]):
    value: V


@dataclass(frozen=True)
class _Meta:
    meta: WindowMetadata


_WindowEvent: TypeAlias = Tuple[int, Union[_Late[V], _Emit[W], _Meta]]


@dataclass
class _WindowLogic(
    StatefulBatchLogic[V, _WindowEvent[V, W], _WindowSnapshot[SC, SW, S]],
):
    clock: ClockLogic[V, SC]
    windower: WindowerLogic[SW]
    builder: Callable[[Optional[S]], WindowLogic[V, W, S]]
    logics: Dict[int, WindowLogic[V, W, S]]

    def _handle_merged(self) -> Iterable[_WindowEvent[V, W]]:
        for orig_window_id, targ_window_id in self.windower.merged():
            if targ_window_id != orig_window_id:
                orig_logic = self.logics.pop(orig_window_id)
                into_logic = self.logics[targ_window_id]
                yield from (
                    (targ_window_id, _Emit(w)) for w in into_logic.on_merge(orig_logic)
                )

            yield (targ_window_id, _Meta(self.windower.metadata_for(targ_window_id)))

    def _handle_closed(self, watermark: datetime) -> Iterable[_WindowEvent[V, W]]:
        for window_id in self.windower.close_for(watermark):
            logic = self.logics.pop(window_id)
            yield from ((window_id, _Emit(w)) for w in logic.on_close())

    @override
    def on_batch(self, values: List[V]) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        self.clock.on_batch()
        events: List[_WindowEvent[V, W]] = []

        for value in values:
            value_timestamp, watermark = self.clock.on_item(value)

            # Attempt to insert into relevant windows.
            in_windows, late_windows = self.windower.open_for(
                value_timestamp, watermark
            )

            for window_id in in_windows:
                if window_id in self.logics:
                    logic = self.logics[window_id]
                else:
                    logic = self.builder(None)
                    self.logics[window_id] = logic
                    events.append(
                        (window_id, _Meta(self.windower.metadata_for(window_id)))
                    )

                events.extend((window_id, _Emit(w)) for w in logic.on_value(value))

            for window_id in late_windows:
                events.append((window_id, _Late(value)))

        # Handle merges. Only need to do once per batch.
        events.extend(self._handle_merged())
        # Handle closes since we're awake.
        events.extend(self._handle_closed(watermark))

        return (events, self.windower.is_empty())

    @override
    def on_notify(self) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        watermark = self.clock.on_notify()
        events = list(self._handle_closed(watermark))
        return (events, self.windower.is_empty())

    @override
    def on_eof(self) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        watermark = self.clock.on_eof()
        events = list(self._handle_closed(watermark))
        return (events, self.windower.is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        return self.windower.notify_at()

    @override
    def snapshot(self) -> _WindowSnapshot[SC, SW, S]:
        return _WindowSnapshot(
            self.clock.snapshot(),
            self.windower.snapshot(),
            {window_id: logic.snapshot() for window_id, logic in self.logics.items()},
        )


@dataclass(frozen=True)
class WindowOut(Generic[V, W]):
    """Streams returned from a windowing operator."""

    down: KeyedStream[Tuple[int, W]]
    """Items emitted from this operator.

    Sub-keyed by window ID.
    """

    late: KeyedStream[Tuple[int, V]]
    """Upstreams items that were deemed late for a window.

    Sub-keyed by window ID."""

    meta: KeyedStream[Tuple[int, WindowMetadata]]
    """Metadata about opened windows.

    Will contain updates to metadata if merges occur.

    Sub-keyed by window ID.
    """


def _unwrap_emit(id_event: _WindowEvent[V, W]) -> Optional[Tuple[int, W]]:
    window_id, event = id_event
    if isinstance(event, _Emit):
        return (window_id, event.value)
    return None


def _unwrap_late(id_event: _WindowEvent[V, W]) -> Optional[Tuple[int, V]]:
    window_id, event = id_event
    if isinstance(event, _Late):
        return (window_id, event.value)
    return None


def _unwrap_meta(id_event: _WindowEvent[V, W]) -> Optional[Tuple[int, WindowMetadata]]:
    window_id, event = id_event
    if isinstance(event, _Meta):
        return (window_id, event.meta)
    return None


@operator
def window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    builder: Callable[[Optional[S]], WindowLogic[V, W, S]],
) -> WindowOut[V, W]:
    """Advanced generic windowing operator.

    This is a lower-level operator Bytewax provideds and gives you
    control over when a windowing operator emits items.
    {py:obj}`WindowLogic` works in tandem with {py:obj}`ClockLogic`
    and {py:obj}`WindowerLogic` to implement its behavior. See
    documentation of those interfaces.

    :arg step_id: Unique ID.

    :arg up: Keyed upstream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg builder: Called whenever a new window is opened with the
        resume state returned from {py:obj}`WindowLogic.snapshot` for
        that window, if any. This should close over any non-state
        configuration and combine it with the resume state to return a
        prepared {py:obj}`WindowLogic` for this window.

    :returns: Window result streams.

    """

    def shim_builder(
        resume_state: Optional[_WindowSnapshot],
    ) -> _WindowLogic[V, W, SC, SW, S]:
        if resume_state is not None:
            clock_logic = clock.build(resume_state.clock_state)
            windower_logic = windower.build(resume_state.windower_state)
            logics = {
                window_id: builder(logic_state)
                for window_id, logic_state in resume_state.logic_states.items()
            }
        else:
            clock_logic = clock.build(None)
            windower_logic = windower.build(None)
            logics = {}
        return _WindowLogic(clock_logic, windower_logic, builder, logics)

    events = op.stateful_batch("stateful_batch", up, shim_builder)
    return WindowOut(
        events.then(op.filter_map_value, "unwrap_down", _unwrap_emit),
        events.then(op.filter_map_value, "unwrap_late", _unwrap_late),
        events.then(op.filter_map_value, "unwrap_meta", _unwrap_meta),
    )


def _collect_list_folder(s: List[V], v: V) -> List[V]:
    s.append(v)
    return s


def _collect_set_folder(s: Set[V], v: V) -> Set[V]:
    s.add(v)
    return s


def _collect_dict_merger(a: Dict[DK, DV], b: Dict[DK, DV]) -> Dict[DK, DV]:
    a.update(b)
    return a


def _collect_get_callbacks(
    step_id: str, t: Type
) -> Tuple[Callable, Callable, Callable]:
    if issubclass(t, list):
        return (list, _collect_list_folder, list.__add__)
    elif issubclass(t, set):
        return (set, _collect_set_folder, set.union)
    elif issubclass(t, dict):

        def dict_folder(d: Dict[DK, DV], k_v: Tuple[DK, DV]) -> Dict[DK, DV]:
            try:
                k, v = k_v
                d[k] = v
                return d
            except TypeError as ex:
                msg = (
                    f"step {step_id!r} collecting into a `dict` "
                    "requires `(key, value)` 2-tuple as the values in the stream; "
                    f"got a {type(k_v)!r} instead"
                )
                raise TypeError(msg) from ex

        return (dict, dict_folder, _collect_dict_merger)
    else:
        msg = (
            f"`collect_window` doesn't support `{t:!}`; "
            "only `list`, `set`, and `dict`; use `fold` operator directly"
        )
        raise TypeError(msg)


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
) -> WindowOut[V, List[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    into: Type[List],
) -> WindowOut[V, List[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    into: Type[Set],
) -> WindowOut[V, Set[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[Tuple[DK, DV]],
    clock: Clock,
    windower: Windower,
    into: Type[Dict],
) -> WindowOut[Tuple[DK, DV], Dict[DK, DV]]: ...


@operator
def collect_window(
    step_id: str,
    up: KeyedStream[X],
    clock: Clock,
    windower: Windower,
    into=list,
) -> WindowOut:
    """Collect items in a window into a container.

    See {py:obj}`bytewax.operators.collect` for the ability to set a
    max size.

    :arg step_id: Unique ID.

    :arg up: Stream of items to count.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg into: Type to collect into. Defaults to {py:obj}`list`.

    :returns: Window result of the collected containers at the end of
        each window.

    """
    shim_builder, shim_folder, shim_merger = _collect_get_callbacks(step_id, into)

    return fold_window(
        "fold_window", up, clock, windower, shim_builder, shim_folder, shim_merger
    )


@operator
def count_window(
    step_id: str,
    up: Stream[X],
    clock: Clock,
    windower: Windower,
    key: Callable[[X], str],
) -> WindowOut[X, int]:
    """Count the number of occurrences of items in a window.

    :arg step_id: Unique ID.

    :arg up: Stream of items to count.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg key: Function to convert each item into a string key. The
        counting machinery does not compare the items directly,
        instead it groups by this string key.

    :returns: Window result of `(key, count)` per window at the end of
        each window.

    """
    keyed = op.key_on("keyed", up, key)
    return fold_window(
        "sum",
        keyed,
        clock,
        windower,
        lambda: 0,
        lambda s, _: s + 1,
        lambda s, t: s + t,
    )


@dataclass
class _FoldWindowLogic(WindowLogic[V, S, S]):
    folder: Callable[[S, V], S]
    merger: Callable[[S, S], S]
    state: S

    @override
    def on_value(self, value: V) -> Iterable[S]:
        self.state = self.folder(self.state, value)
        return _EMPTY

    @override
    def on_merge(self, consume: Self) -> Iterable[S]:
        self.state = self.merger(self.state, consume.state)
        return _EMPTY

    @override
    def on_close(self) -> Iterable[S]:
        return (self.state,)

    @override
    def snapshot(self) -> S:
        return copy.deepcopy(self.state)


def fold_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    builder: Callable[[], S],
    folder: Callable[[S, V], S],
    merger: Callable[[S, S], S],
) -> WindowOut[V, S]:
    """Build an empty accumulator, then combine values into it.

    It is like {py:obj}`reduce_window` but uses a function to build
    the initial value.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg builder: Called the first time a key appears and is expected
        to return the empty accumulator for that key.

    :arg folder: Combines a new value into an existing accumulator and
        returns the updated accumulator. The accumulator is initially
        the empty accumulator.

    :arg merger: Combines two states whenever two windows merge. Not
        all window definitions result in merges.

    :returns: Window result of the accumulators once each window has
        closed.

    """

    def shim_builder(resume_state: Optional[S]) -> _FoldWindowLogic[V, S]:
        state = resume_state if resume_state is not None else builder()
        return _FoldWindowLogic(folder, merger, state)

    return window("generic_window", up, clock, windower, shim_builder)


def _join_window_folder(state: _JoinState, name_value: Tuple[str, Any]) -> _JoinState:
    name, value = name_value
    state.set_val(name, value)
    return state


def _join_window_product_folder(
    state: _JoinState, name_value: Tuple[str, Any]
) -> _JoinState:
    name, value = name_value
    state.add_val(name, value)
    return state


def _join_astuples_flat_mapper(
    meta_state: Tuple[int, _JoinState],
) -> Iterable[Tuple[int, Tuple]]:
    window_id, state = meta_state
    for t in state.astuples():
        yield (window_id, t)


def _join_asdicts_flat_mapper(
    meta_state: Tuple[int, _JoinState],
) -> Iterable[Tuple[int, Dict]]:
    window_id, state = meta_state
    for d in state.asdicts():
        yield (window_id, d)


def _join_merger(s: _JoinState, t: _JoinState) -> _JoinState:
    s += t
    return s


@operator
def join_window(
    step_id: str,
    clock: Clock,
    windower: Windower,
    *sides: KeyedStream[Any],
    product: bool = False,
) -> WindowOut[Any, Tuple]:
    """Gather together the value for a key on multiple streams.

    :arg step_id: Unique ID.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg *sides: Keyed streams.

    :arg product: When `True`, emit all combinations of all values
        seen on all sides. E.g. if side 1 saw `"A"` and `"B"`, and
        side 2 saw `"C"`: emit `("A", "C")`, `("B", "C")` downstream.
        Defaults to `False`.

    :returns: Window result with tuples with the value from each
        stream in the order of the argument list once each window has
        closed.

    """
    named_sides = dict((str(i), s) for i, s in enumerate(sides))
    names = list(named_sides.keys())

    merged = op._join_name_merge("add_names", **named_sides)

    def builder() -> _JoinState:
        return _JoinState.for_names(names)

    # TODO: Egregious hack. Remove when we refactor to have timestamps
    # in stream.
    if isinstance(clock, EventClock):
        value_ts_getter = clock.ts_getter

        def shim_getter(i_v):
            _, v = i_v
            return value_ts_getter(v)

        clock = EventClock(
            ts_getter=shim_getter,
            wait_for_system_duration=clock.wait_for_system_duration,
        )

    if not product:
        folder = _join_window_folder
    else:
        folder = _join_window_product_folder

    joined_out = fold_window(
        "fold_window",
        merged,
        clock,
        windower,
        builder,
        folder,
        _join_merger,
    )
    return WindowOut(
        op.flat_map_value("astuple", joined_out.down, _join_astuples_flat_mapper),
        joined_out.late,
        joined_out.meta,
    )


@operator
def join_window_named(
    step_id: str,
    clock: Clock,
    windower: Windower,
    product: bool = False,
    **sides: KeyedStream[Any],
) -> WindowOut[Any, Dict]:
    """Gather together the value for a key on multiple named streams.

    :arg step_id: Unique ID.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg product: When `True`, emit all combinations of all values
        seen on all sides. E.g. if side `right` saw `"A"` and `"B"`,
        and side `left` saw `"C"`: emit `{"right": "A", "left": "C"}`,
        `{"right": "B", "left": "C"}` downstream. Defaults to `False`.

    :arg **sides: Named keyed streams. The name of each stream will be
        used in the emitted {py:obj}`dict`s.

    :returns: Window result with a {py:obj}`dict` mapping the name to
        the value from each stream once each window has closed.

    """
    names = list(sides.keys())

    merged = op._join_name_merge("add_names", **sides)

    def builder() -> _JoinState:
        return _JoinState.for_names(names)

    # TODO: Egregious hack. Remove when we refactor to have timestamps
    # in stream.
    if isinstance(clock, EventClock):
        value_ts_getter = clock.ts_getter

        def shim_getter(i_v):
            _, v = i_v
            return value_ts_getter(v)

        clock = EventClock(
            ts_getter=shim_getter,
            wait_for_system_duration=clock.wait_for_system_duration,
        )

    if not product:
        folder = _join_window_folder
    else:
        folder = _join_window_product_folder

    joined_out = fold_window(
        "fold_window",
        merged,
        clock,
        windower,
        builder,
        folder,
        _join_merger,
    )
    return WindowOut(
        op.flat_map_value("asdict", joined_out.down, _join_asdicts_flat_mapper),
        joined_out.late,
        joined_out.meta,
    )


@overload
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
) -> WindowOut[V, V]: ...


@overload
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    by: Callable[[V], Any],
) -> WindowOut[V, V]: ...


@operator
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    by=_identity,
) -> WindowOut[V, V]:
    """Find the maximum value for each key.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: Window result of the max values once each window has
        closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(max, key=by))


@overload
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
) -> WindowOut[V, V]: ...


@overload
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    by: Callable[[V], Any],
) -> WindowOut[V, V]: ...


@operator
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    by=_identity,
) -> WindowOut[V, V]:
    """Find the minumum value for each key.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: Window result of the min values once each window has
        closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(min, key=by))


@operator
def reduce_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock,
    windower: Windower,
    reducer: Callable[[V, V], V],
) -> WindowOut[V, V]:
    """Distill all values for a key down into a single value.

    It is like {py:obj}`fold_window` but the first value is the
    initial accumulator.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg reducer: Combines a new value into an old value and returns
        the combined value.

    :returns: Window result of the reduced values once each window has
        closed.

    """

    def shim_folder(s: V, v: V) -> V:
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return fold_window(
        "fold_window", up, clock, windower, _untyped_none, shim_folder, reducer
    )
