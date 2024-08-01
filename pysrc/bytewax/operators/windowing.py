"""Time-based windowing operators."""

import copy
import typing
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
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import bytewax.operators as op
from bytewax._utils import partition
from bytewax.dataflow import (
    Stream,
    operator,
)
from bytewax.operators import (
    _EMPTY,
    DK,
    DV,
    JoinEmitMode,
    JoinInsertMode,
    KeyedStream,
    S,
    StatefulBatchLogic,
    U,
    V,
    W,
    W_co,
    X,
    _get_system_utc,
    _identity,
    _JoinState,
    _untyped_none,
)
from typing_extensions import Self, TypeAlias, override

ZERO_TD: timedelta = timedelta(seconds=0)
"""A zero length of time."""


UTC_MAX: datetime = datetime.max.replace(tzinfo=timezone.utc)
"""Maximum possible UTC date time."""


UTC_MIN: datetime = datetime.min.replace(tzinfo=timezone.utc)
"""Minimum possible UTC date time."""


SC = TypeVar("SC")
"""Type of {py:obj}`ClockLogic`'s state snapshots."""


SW = TypeVar("SW")
"""Type of {py:obj}`WindowerLogic`'s state snapshots."""


C = TypeVar("C", bound=Iterable)
"""Type of downstream containers."""


class ClockLogic(ABC, Generic[V, S]):
    """Abstract class to define a sense of time for windowing.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Clock` for built-in options.

    This type is instantiated within Bytewax's windowing operators and
    has methods called by the runtime. A new subclass of this will
    need to be paired with a new subclass of {py:obj}`Clock`, which
    holds the configuration data for this type.

    This is instantiated for each key which is encountered.

    """

    @abstractmethod
    def before_batch(self) -> None:
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
    def to_system_utc(self, timestamp: datetime) -> Optional[datetime]:
        """Convert a timestamp to a UTC system time.

        This is used to determine when the runtime should wake up to
        check for closed windows.

        :arg timestamp: Previously returned by `on_item`.

        :returns: A system time in UTC. If `None`, signal that there
            is no relevant system time to wake at.

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
class _SystemClockLogic(ClockLogic[Any, None]):
    now_getter: Callable[[], datetime]
    _now: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._now = self.now_getter()

    @override
    def before_batch(self) -> None:
        self._now = self.now_getter()

    @override
    def on_item(self, value: Any) -> Tuple[datetime, datetime]:
        return (self._now, self._now)

    @override
    def on_notify(self) -> datetime:
        self._now = self.now_getter()
        return self._now

    @override
    def on_eof(self) -> datetime:
        return UTC_MAX

    @override
    def to_system_utc(self, timestamp: datetime) -> Optional[datetime]:
        return timestamp

    @override
    def snapshot(self) -> None:
        return None


@dataclass
class _EventClockState:
    system_time_of_max_event: datetime
    watermark_base: datetime


@dataclass
class _EventClockLogic(ClockLogic[V, _EventClockState]):
    now_getter: Callable[[], datetime]
    timestamp_getter: Callable[[V], datetime]
    to_system: Callable[[datetime], Optional[datetime]]
    wait_for_system_duration: timedelta

    state: _EventClockState = field(
        default_factory=lambda: _EventClockState(
            system_time_of_max_event=UTC_MIN,
            watermark_base=UTC_MIN,
        )
    )
    _system_now: datetime = field(init=False)

    def __post_init__(self) -> None:
        self._system_now = self.now_getter()
        if self.state.system_time_of_max_event <= UTC_MIN:
            self.state.system_time_of_max_event = self._system_now

    @override
    def before_batch(self) -> None:
        system_now = self.now_getter()
        assert system_now >= self._system_now
        self._system_now = system_now

    @override
    def on_item(self, value: V) -> Tuple[datetime, datetime]:
        value_event_timestamp = self.timestamp_getter(value)

        watermark = self.state.watermark_base + (
            self._system_now - self.state.system_time_of_max_event
        )

        try:
            watermark_base = value_event_timestamp - self.wait_for_system_duration
            if watermark_base > watermark:
                assert watermark_base >= self.state.watermark_base
                self.state.watermark_base = watermark_base

                assert self._system_now >= self.state.system_time_of_max_event
                self.state.system_time_of_max_event = self._system_now

                return value_event_timestamp, watermark_base
        except OverflowError:
            # Since the new actual watermark is unrepresentable, we
            # need to keep the watermark advancing at UTC_MIN + system
            # time, so that the watermark does not regress.
            pass

        return value_event_timestamp, watermark

    @override
    def on_notify(self) -> datetime:
        if self.state is None:
            return UTC_MIN
        else:
            self.before_batch()
            watermark = self.state.watermark_base + (
                self._system_now - self.state.system_time_of_max_event
            )
            return watermark

    @override
    def on_eof(self) -> datetime:
        return UTC_MAX

    @override
    def to_system_utc(self, timestamp: datetime) -> Optional[datetime]:
        return self.to_system(timestamp)

    @override
    def snapshot(self) -> _EventClockState:
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
class SystemClock(Clock[Any, None]):
    """Uses the current system time as the timestamp for each item.

    The watermark is the current system time.

    All timestamps and watermarks are generated in UTC.

    When the dataflow has no more input, all windows are closed.

    """

    @override
    def build(self, resume_state: None) -> _SystemClockLogic:
        return _SystemClockLogic(_get_system_utc)


@dataclass
class EventClock(Clock[V, _EventClockState]):
    """Use a timestamp embedded within each item.

    The watermark is the largest timestamp seen thus far, minus the
    waiting duration, plus the system time duration that has elapsed
    since that item was seen. This effectively means that items will
    be correctly processed as long as they do not have timestamps
    out-of-order or arrive later than the waiting duration.

    :::{note}

    By default, all timestamps returned by `ts_getter` also need to be
    [aware datetimes](inv:python:std:label#datetime-naive-aware) in
    UTC.

    This is because `now_getter` defaults to returning the current
    system time as [aware
    datetimes](inv:python:std:label#datetime-naive-aware) in UTC, and
    comparisons are made to that.

    It's easiest to convert your timestamps to UTC in `ts_getter`, but
    you can also specify a custom `now_getter` and `to_system_utc` to
    support other sense of time.

    :::

    :arg ts_getter: Called once on each item to get its timestamp.

    :arg wait_for_system_duration: How much system time to wait after
        seeing a timestamp to have the watermark catch up to that time.

    :arg now_getter: Return the current "system" timestamp used to
        advance the watermark. Defaults to the current system time in
        UTC.

    :arg to_system_utc: For a given window close timestamp, return the
        corresponding UTC system time that the runtime should use to
        know when to wake up to run window close logic. If `None` is
        returned, the runtime will not wake up to close windows and
        will depend on new items or EOF to trigger any closes.
        Defaults to passing through the timestamp.

    """

    ts_getter: Callable[[V], datetime]
    wait_for_system_duration: timedelta
    now_getter: Callable[[], datetime] = _get_system_utc
    to_system_utc: Callable[[datetime], Optional[datetime]] = _identity

    @override
    def build(self, resume_state: Optional[_EventClockState]) -> _EventClockLogic[V]:
        if resume_state is None:
            return _EventClockLogic(
                self.now_getter,
                self.ts_getter,
                self.to_system_utc,
                self.wait_for_system_duration,
            )
        else:
            return _EventClockLogic(
                self.now_getter,
                self.ts_getter,
                self.to_system_utc,
                self.wait_for_system_duration,
                resume_state,
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
    merged_ids: Set[int] = field(default_factory=set)
    """Any original window IDs merged into this window before close."""


class WindowerLogic(ABC, Generic[S]):
    """Abstract class which defines a type of window.

    If you're just getting started with windowing operators, see the
    concrete subclasses of {py:obj}`Windower` for built-in options.

    This type is instantiated within Bytewax's windowing operators and
    has methods called by the runtime. A new subclass of this will
    need to be paired with a new subclass of {py:obj}`Windower`, which
    holds the configuration data for this type.

    This is instantiated for each key which is encountered.

    """

    @abstractmethod
    def open_for(
        self,
        timestamp: datetime,
    ) -> Iterable[int]:
        """Find which windows an item is in and mark them as open.

        Called when a new, non-late item arrives.

        You'll need to do whatever bookkeeping is necessary internally
        to be able to satisfy the other abstract methods, like being
        able to return the metadata for a window.

        :arg timestamp: Of the incoming item.

        :returns: A list of window IDs that this item is in.

        """
        ...

    @abstractmethod
    def late_for(self, timestamp: datetime) -> Iterable[int]:
        """Find which windows an item would have been in, if on-time.

        Called when a late item arrives.

        If there isn't a way to know for sure which specific windows
        this item would have been in, you can return a sentinel value.

        This should not persist any state internally for the returned
        window IDs; the watermark has already passed these windows and
        so they will not be processed by {py:obj}`close_for`.

        :arg timestamp: Of the incoming item.

        :returns: A list of window IDs that this item is late for.

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
        been returned as an original window, that ID does not need to
        be reported as closed.

        :returns: An iterable of 2-tuples of `(original_window_id,
            target_window_id)`.

        """
        ...

    @abstractmethod
    def close_for(self, watermark: datetime) -> Iterable[Tuple[int, WindowMetadata]]:
        """Report what windows are now closed.

        Called periodically once the watermark has advanced.

        Once a window ID is returned from this method, it should never
        be opened again (although items may be reported as late for
        it). Internally, you can discard whatever state was being
        maintained about this window.

        :arg watermark: Current watermark. Guaranteed to never
            regress across calls.


        :returns: A list of now closed window IDs and the their final
            metadata.

        """
        ...

    @abstractmethod
    def notify_at(self) -> Optional[datetime]:
        """Next system time timestamp for which a window might close.

        This will be called periodically to know when to wake up this
        operator to close windows, even if no items were received.

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

    state: _SlidingWindowerState

    def intersects(self, timestamp: datetime) -> List[int]:
        since_origin = timestamp - self.align_to
        return list(
            range(
                (since_origin - self.length) // self.offset + 1,
                since_origin // self.offset + 1,
            )
        )

    def _metadata_for(self, window_id: int) -> WindowMetadata:
        open_time = self.align_to + self.offset * window_id
        close_time = open_time + self.length
        return WindowMetadata(open_time, close_time)

    @override
    def open_for(self, timestamp: datetime) -> List[int]:
        in_windows = []
        for window_id in self.intersects(timestamp):
            meta = self.state.opened.get(window_id) or self._metadata_for(window_id)
            self.state.opened.setdefault(window_id, meta)
            in_windows.append(window_id)

        return in_windows

    @override
    def late_for(self, timestamp: datetime) -> List[int]:
        return [window_id for window_id in self.intersects(timestamp)]

    @override
    def merged(self) -> Iterable[Tuple[int, int]]:
        """Sliding windows never merge."""
        return _EMPTY

    @override
    def close_for(self, watermark: datetime) -> Iterable[Tuple[int, WindowMetadata]]:
        closed = [
            (window_id, meta)
            for window_id, meta in self.state.opened.items()
            if meta.close_time <= watermark
        ]
        for window_id, _meta in closed:
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
"""Sentinel window ID assigned to late items."""


@dataclass
class _SessionWindowerState:
    max_key: int = LATE_SESSION_ID
    sessions: Dict[int, WindowMetadata] = field(default_factory=dict)
    merge_queue: List[Tuple[int, int]] = field(default_factory=list)
    """Original ID to new ID."""


def _open_sort(id_meta: Tuple[int, WindowMetadata]) -> datetime:
    _window_id, meta = id_meta
    return meta.open_time


def _session_find_merges(
    sessions: Dict[int, WindowMetadata],
    gap: timedelta,
) -> List[Tuple[int, int]]:
    """Mutates sessions in place."""
    merge_queue = []

    sorted_id_sessions = sorted(sessions.items(), key=_open_sort)
    last_window_id, last_meta = sorted_id_sessions.pop(0)
    for this_id, this_meta in sorted_id_sessions:
        # See if this next window is within the gap from the
        # last close. Since we sorted by open time, this is
        # always the correct direction.
        if this_meta.open_time - last_meta.close_time <= gap:
            # If so, it's a merge. Extend the close time (if
            # it actually expands it; the merged window might
            # be fully within the last window).
            last_meta.close_time = max(last_meta.close_time, this_meta.close_time)
            merge_queue.append((this_id, last_window_id))
            last_meta.merged_ids.add(this_id)
            del sessions[this_id]
        else:
            # If they don't merge, this current window is now
            # the new last window to attempt to merge with in
            # the next iteration.
            last_window_id, last_meta = (this_id, this_meta)

    return merge_queue


@dataclass
class _SessionWindowerLogic(WindowerLogic[_SessionWindowerState]):
    gap: timedelta
    state: _SessionWindowerState

    def _find_merges(self) -> None:
        if len(self.state.sessions) >= 2:
            new_merges = _session_find_merges(
                self.state.sessions,
                self.gap,
            )
            self.state.merge_queue.extend(new_merges)

    @override
    def open_for(self, timestamp: datetime) -> Iterable[int]:
        for window_id, meta in self.state.sessions.items():
            assert meta.open_time <= meta.close_time
            until_open = meta.open_time - timestamp
            since_close = timestamp - meta.close_time
            if until_open <= ZERO_TD and since_close <= ZERO_TD:
                # If we're perfectly within an existing session,
                # re-use that session. No merges possible because no
                # session boundaries changed.
                return (window_id,)
            elif until_open > ZERO_TD and until_open <= self.gap:
                # If we're within the gap before an existing session,
                # re-use the session ID, but also see if any merges
                # occurred.
                meta.open_time = timestamp
                self._find_merges()
                return (window_id,)
            elif since_close > ZERO_TD and since_close <= self.gap:
                # Same if after an existing session.
                meta.close_time = timestamp
                self._find_merges()
                return (window_id,)

        # If we're outside all existing sessions, make anew. No need
        # to merge because we aren't within the gap of any existing
        # session.
        self.state.max_key += 1
        window_id = self.state.max_key
        self.state.sessions[window_id] = WindowMetadata(timestamp, timestamp)
        # Items are never late for a session window.
        return (window_id,)

    @override
    def late_for(self, timestamp: datetime) -> Iterable[int]:
        return (LATE_SESSION_ID,)

    @override
    def merged(self) -> Iterable[Tuple[int, int]]:
        merged = self.state.merge_queue
        self.state.merge_queue = []
        return merged

    @override
    def close_for(self, watermark: datetime) -> Iterable[Tuple[int, WindowMetadata]]:
        try:
            close_after = watermark - self.gap
        except OverflowError:
            close_after = UTC_MIN
        closed = [
            (window_id, meta)
            for window_id, meta in self.state.sessions.items()
            if meta.close_time < close_after
        ]
        for window_id, _meta in closed:
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

    :::{warning}

    Changing any of the windowing parameters across executions will
    result in incorrect output for any windows that were open at the
    time of the resume snapshot.

    :::

    :arg length: Length of windows.

    :arg offset: Duration between start times of adjacent windows.

    :arg align_to: Align windows so this instant starts a window. You
        can use this to align all windows to an hour boundary, e.g.

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

    :::{warning}

    Changing any of the windowing parameters across executions will
    result in incorrect output for any windows that were open at the
    time of the resume snapshot.

    :::

    :arg length: Length of windows.

    :arg align_to: Align windows so this instant starts a window. You
        can use this to align all windows to an hour boundary, e.g.

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
            msg = "session window `gap` must not be negative"
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

        This will be called with values in timestamp order.

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


_WindowQueueEntry: TypeAlias = Tuple[V, datetime]


@dataclass(frozen=True)
class _WindowSnapshot(Generic[V, SC, SW, S]):
    clock_state: SC
    windower_state: SW
    logic_states: Dict[int, S]
    queue: List[_WindowQueueEntry[V]]


_Emit: TypeAlias = Tuple[int, Literal["E"], V]
_Late: TypeAlias = Tuple[int, Literal["L"], V]
_Meta: TypeAlias = Tuple[int, Literal["M"], WindowMetadata]
_WindowEvent: TypeAlias = Union[_Late[V], _Emit[W], _Meta]


@dataclass
class _WindowLogic(
    StatefulBatchLogic[
        V,
        _WindowEvent[V, W],
        _WindowSnapshot[V, SC, SW, S],
    ],
):
    clock: ClockLogic[V, SC]
    windower: WindowerLogic[SW]
    builder: Callable[[Optional[S]], WindowLogic[V, W, S]]
    ordered: bool

    logics: Dict[int, WindowLogic[V, W, S]] = field(default_factory=dict)
    queue: List[_WindowQueueEntry[V]] = field(default_factory=list)

    _last_watermark: datetime = UTC_MIN

    def _handle_inserts(
        self, due_entries: List[_WindowQueueEntry[V]]
    ) -> Iterable[_WindowEvent[V, W]]:
        for entry in due_entries:
            value, timestamp = entry
            for window_id in self.windower.open_for(timestamp):
                if window_id in self.logics:
                    logic = self.logics[window_id]
                else:
                    logic = self.builder(None)
                    self.logics[window_id] = logic

                ws = logic.on_value(value)
                yield from ((window_id, "E", w) for w in ws)

    def _handle_merged(self) -> Iterable[_WindowEvent[V, W]]:
        for orig_window_id, targ_window_id in self.windower.merged():
            if targ_window_id != orig_window_id:
                orig_logic = self.logics.pop(orig_window_id)
                into_logic = self.logics[targ_window_id]
                ws = into_logic.on_merge(orig_logic)
                yield from ((targ_window_id, "E", w) for w in ws)

    def _handle_closed(self, watermark: datetime) -> Iterable[_WindowEvent[V, W]]:
        for window_id, meta in self.windower.close_for(watermark):
            logic = self.logics.pop(window_id)
            ws = logic.on_close()
            yield from ((window_id, "E", w) for w in ws)

            yield (window_id, "M", meta)

    def _flush_queue(self, watermark: datetime) -> Iterable[_WindowEvent[V, W]]:
        if self.ordered:
            # Remove due entries from the queue.
            due_entries, self.queue = partition(
                self.queue, lambda entry: entry[1] <= watermark
            )
            due_entries.sort(key=lambda entry: entry[1])
        else:
            due_entries = self.queue
            self.queue = []

        yield from self._handle_inserts(due_entries)
        yield from self._handle_merged()
        yield from self._handle_closed(watermark)

    def _is_empty(self) -> bool:
        return (
            len(self.logics) <= 0 and len(self.queue) <= 0 and self.windower.is_empty()
        )

    @override
    def on_batch(self, values: List[V]) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        self.clock.before_batch()

        events: List[_WindowEvent[V, W]] = []
        for value in values:
            value_timestamp, watermark = self.clock.on_item(value)
            assert watermark >= self._last_watermark
            self._last_watermark = watermark

            if value_timestamp < watermark:
                late_window_ids = self.windower.late_for(value_timestamp)
                events.extend((window_id, "L", value) for window_id in late_window_ids)
            else:
                entry = (value, value_timestamp)
                self.queue.append(entry)

        events.extend(self._flush_queue(watermark))
        return (events, self._is_empty())

    @override
    def on_notify(self) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        watermark = self.clock.on_notify()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = list(self._flush_queue(watermark))
        return (events, self._is_empty())

    @override
    def on_eof(self) -> Tuple[Iterable[_WindowEvent[V, W]], bool]:
        watermark = self.clock.on_eof()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = list(self._flush_queue(watermark))
        return (events, self._is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        notify_at = self.windower.notify_at()
        if notify_at is not None:
            notify_at = self.clock.to_system_utc(notify_at)
        return notify_at

    @override
    def snapshot(self) -> _WindowSnapshot[V, SC, SW, S]:
        return _WindowSnapshot(
            self.clock.snapshot(),
            self.windower.snapshot(),
            {window_id: logic.snapshot() for window_id, logic in self.logics.items()},
            # No need to deepcopy since entries are frozen.
            list(self.queue),
        )


@dataclass(frozen=True)
class WindowOut(Generic[V, W_co]):
    """Streams returned from a windowing operator."""

    down: KeyedStream[Tuple[int, W_co]]
    """Items emitted from this operator.

    Sub-keyed by window ID.
    """

    late: KeyedStream[Tuple[int, V]]
    """Upstreams items that were deemed late for a window.

    It's possible you will see window IDs here that were never in the
    `down` or `meta` streams, depending on the specifics of the
    ordering of the data.

    Sub-keyed by window ID."""

    meta: KeyedStream[Tuple[int, WindowMetadata]]
    """Metadata about closed windows.

    Emitted once when that window closes. Not emitted for original
    windows that are merged into another window. The target window's
    {py:obj}`~bytewax.operators.windowing.WindowMetadata` will have
    the original window IDs in
    {py:obj}`~bytewax.operators.windowing.WindowMetadata.merged_ids`.

    Sub-keyed by window ID.
    """


def _unwrap_emit(id_typ_obj: _WindowEvent[V, W]) -> Optional[Tuple[int, W]]:
    window_id, typ, obj = id_typ_obj
    if typ == "E":
        value = cast(W, obj)
        return (window_id, value)
    else:
        return None


def _unwrap_late(id_typ_obj: _WindowEvent[V, W]) -> Optional[Tuple[int, V]]:
    window_id, typ, obj = id_typ_obj
    if typ == "L":
        value = cast(V, obj)
        return (window_id, value)
    else:
        return None


def _unwrap_meta(
    id_typ_obj: _WindowEvent[V, W],
) -> Optional[Tuple[int, WindowMetadata]]:
    window_id, typ, obj = id_typ_obj
    if typ == "M":
        meta = cast(WindowMetadata, obj)
        return (window_id, meta)
    else:
        return None


@operator
def window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    builder: Callable[[Optional[S]], WindowLogic[V, W, S]],
    ordered: bool = True,
) -> WindowOut[V, W]:
    """Advanced generic windowing operator.

    This is a lower-level operator Bytewax and gives you control over
    when a windowing operator emits items. {py:obj}`WindowLogic` works
    in tandem with {py:obj}`ClockLogic` and {py:obj}`WindowerLogic` to
    implement its behavior. See documentation of those interfaces.

    :arg step_id: Unique ID.

    :arg up: Keyed upstream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg builder: Called whenever a new window is opened with the
        resume state returned from {py:obj}`WindowLogic.snapshot` for
        that window, if any. This should close over any non-state
        configuration and combine it with the resume state to return a
        prepared {py:obj}`WindowLogic` for this window.

    :arg ordered: Whether to apply values to the logic in timestamp
        order. If not, they'll be in upstream order. There is a
        performance and latency penalty to ordering by timestamp.
        Defaults to `True`.

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
            return _WindowLogic(
                clock_logic,
                windower_logic,
                builder,
                ordered,
                logics,
                resume_state.queue,
            )
        else:
            clock_logic = clock.build(None)
            windower_logic = windower.build(None)
            return _WindowLogic(
                clock_logic,
                windower_logic,
                builder,
                ordered,
            )

    events = op.stateful_batch("stateful_batch", up, shim_builder)

    downs: KeyedStream[Tuple[int, W]] = op.filter_map_value(
        "unwrap_down",
        events,
        _unwrap_emit,
    )
    lates: KeyedStream[Tuple[int, V]] = op.filter_map_value(
        "unwrap_late",
        events,
        _unwrap_late,
    )
    metas: KeyedStream[Tuple[int, WindowMetadata]] = op.filter_map_value(
        "unwrap_meta",
        events,
        _unwrap_meta,
    )
    return WindowOut(downs, lates, metas)


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
    clock: Clock[V, Any],
    windower: Windower[Any],
) -> WindowOut[V, List[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    into: Type[List],
) -> WindowOut[V, List[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    into: Type[Set],
) -> WindowOut[V, Set[V]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[Tuple[DK, DV]],
    clock: Clock[Tuple[DK, DV], SC],
    windower: Windower[Any],
    into: Type[Dict],
) -> WindowOut[Tuple[DK, DV], Dict[DK, DV]]: ...


@overload
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    into=list,
) -> WindowOut[V, Any]: ...


@operator
def collect_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    into=list,
    ordered: bool = True,
) -> WindowOut[V, Any]:
    """Collect items in a window into a container.

    See {py:obj}`bytewax.operators.collect` for the ability to set a
    max size.

    :arg step_id: Unique ID.

    :arg up: Stream of items to count.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg into: Type to collect into. Defaults to {py:obj}`list`.

    :arg ordered: Whether values in the resulting containers are in
        timestamp order. There is a performance and latency penalty to
        ordering by timestamp. If not, they'll be in upstream order.
        Defaults to `True`.

    :returns: Window result streams. Downstream contains the collected
        containers with values in timestamp order at the end of each
        window.

    """
    shim_builder, shim_folder, shim_merger = _collect_get_callbacks(step_id, into)

    return fold_window(
        "fold_window",
        up,
        clock,
        windower,
        shim_builder,
        shim_folder,
        shim_merger,
        ordered,
    )


@operator
def count_window(
    step_id: str,
    up: Stream[X],
    clock: Clock[X, SC],
    windower: Windower[Any],
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

    :returns: Window result streams. Downstream contains of `(key,
        count)` per window at the end of each window.

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
        ordered=False,
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


@operator
def fold_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    builder: Callable[[], S],
    folder: Callable[[S, V], S],
    merger: Callable[[S, S], S],
    ordered: bool = True,
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
        the empty accumulator. Values will be passed in timestamp
        order.

    :arg merger: Combines two states whenever two windows merge. Not
        all window definitions result in merges.

    :arg ordered: Whether to fold values in timestamp order. If not,
        they'll be in upstream order. There is a performance and
        latency penalty to ordering by timestamp. Defaults to `True`.

    :returns: Window result streams. Downstream contains the
        accumulator for each window, once that window has closed.

    """

    def shim_builder(resume_state: Optional[S]) -> _FoldWindowLogic[V, S]:
        state = resume_state if resume_state is not None else builder()
        return _FoldWindowLogic(folder, merger, state)

    return window("window", up, clock, windower, shim_builder, ordered)


@dataclass
class _JoinWindowLogic(WindowLogic[Tuple[int, V], Tuple, _JoinState]):
    insert_mode: JoinInsertMode
    emit_mode: JoinEmitMode

    state: _JoinState

    def _check_emit(self) -> Iterable[Tuple]:
        if self.emit_mode == "complete" and self.state.all_set():
            rows = self.state.astuples()
            self.state.clear()
            return rows
        elif self.emit_mode == "running":
            return self.state.astuples()
        else:
            return _EMPTY

    @override
    def on_value(self, value: Tuple[int, V]) -> Iterable[Tuple]:
        join_side, join_value = value
        if self.insert_mode == "first" and not self.state.is_set(join_side):
            self.state.set_val(join_side, join_value)
        elif self.insert_mode == "last":
            self.state.set_val(join_side, join_value)
        elif self.insert_mode == "product":
            self.state.add_val(join_side, join_value)

        return self._check_emit()

    @override
    def on_merge(self, original: Self) -> Iterable[Tuple]:
        if self.insert_mode == "first":
            # Since `self` is the "older" window, only overwrites the
            # current values if they don't exist.
            self.state |= original.state
        elif self.insert_mode == "last":
            # Do the opposite.
            original.state |= self.state
            self.state = original.state
        elif self.insert_mode == "product":
            self.state += original.state

        return self._check_emit()

    @override
    def on_close(self) -> Iterable[Tuple]:
        if self.emit_mode == "final":
            return self.state.astuples()
        else:
            return _EMPTY

    @override
    def snapshot(self) -> _JoinState:
        return copy.deepcopy(self.state)


@overload
def join_window(
    step_id: str,
    clock: Clock[V, Any],
    windower: Windower[Any],
    side1: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"],
) -> WindowOut[V, Tuple[V]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"],
) -> WindowOut[Union[U, V], Tuple[U, V]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V, W], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"],
) -> WindowOut[Union[U, V, W], Tuple[U, V, W]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V, W, X], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    side4: KeyedStream[X],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: Literal["complete"],
) -> WindowOut[Union[U, V, W, X], Tuple[U, V, W, X]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[V, Any],
    windower: Windower[Any],
    side1: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[V, Tuple[Optional[V]]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[Union[U, V], Tuple[Optional[U], Optional[V]]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V, W], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[Union[U, V, W], Tuple[Optional[U], Optional[V], Optional[W]]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Union[U, V, W, X], SC],
    windower: Windower[Any],
    side1: KeyedStream[U],
    side2: KeyedStream[V],
    side3: KeyedStream[W],
    side4: KeyedStream[X],
    /,
    *,
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[
    Union[U, V, W, X], Tuple[Optional[U], Optional[V], Optional[W], Optional[X]]
]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[V, Any],
    windower: Windower[Any],
    *sides: KeyedStream[V],
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[V, Tuple[Optional[V], ...]]: ...


@overload
def join_window(
    step_id: str,
    clock: Clock[Any, SC],
    windower: Windower[Any],
    *sides: KeyedStream[Any],
    insert_mode: JoinInsertMode = ...,
    emit_mode: JoinEmitMode = ...,
) -> WindowOut[Any, Tuple]: ...


@operator
def join_window(
    step_id: str,
    clock: Clock[Any, Any],
    windower: Windower[Any],
    *sides: KeyedStream[Any],
    insert_mode: JoinInsertMode = "last",
    emit_mode: JoinEmitMode = "final",
) -> WindowOut[Any, Tuple]:
    """Gather together the value for a key on multiple streams.

    See <project:#xref-joins> for more information.

    :arg step_id: Unique ID.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg *sides: Keyed streams.

    :arg insert_mode: Mode of this join. See
        {py:obj}`~bytewax.operators.JoinInsertMode` for more info.
        Defaults to `"last"`.

    :arg emit_mode: Mode of this join. See
        {py:obj}`~bytewax.operators.JoinEmitMode` for more info.
        Defaults to `"final"`.


    :returns: Window result streams. Downstream contains tuples with
        the value from each stream in the order of the argument list.
        See {py:obj}`~bytewax.operators.JoinEmitMode` for when tuples
        are emitted.

    """
    if insert_mode not in typing.get_args(JoinInsertMode):
        msg = f"unknown join insert mode {insert_mode!r}"
        raise ValueError(msg)
    if emit_mode not in typing.get_args(JoinEmitMode):
        msg = f"unknown join emit mode {emit_mode!r}"
        raise ValueError(msg)

    side_count = len(sides)

    merged = op._join_label_merge("add_names", *sides)

    def builder() -> _JoinState:
        return _JoinState.for_side_count(side_count)

    # TODO: Egregious hack. Remove when we refactor to have timestamps
    # in stream.
    if isinstance(clock, EventClock):
        value_ts_getter = clock.ts_getter

        def shim_getter(i_v: Tuple[str, Any]) -> datetime:
            _, v = i_v
            return value_ts_getter(v)

        clock = EventClock(
            ts_getter=shim_getter,
            wait_for_system_duration=clock.wait_for_system_duration,
        )

    def shim_builder(
        resume_state: Optional[_JoinState],
    ) -> WindowLogic[Tuple[int, Any], Tuple, _JoinState]:
        if resume_state is None:
            state = _JoinState.for_side_count(side_count)
            return _JoinWindowLogic(insert_mode, emit_mode, state)
        else:
            return _JoinWindowLogic(insert_mode, emit_mode, resume_state)

    return window(
        "window",
        merged,
        clock,
        windower,
        shim_builder,
    )


@overload
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
) -> WindowOut[V, V]: ...


@overload
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    by: Callable[[V], Any],
) -> WindowOut[V, V]: ...


@operator
def max_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    by=_identity,
) -> WindowOut[V, V]:
    """Find the maximum value for each key.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: Window result streams. Downstream contains the max value
        for each window, once that window has closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(max, key=by))


@overload
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
) -> WindowOut[V, V]: ...


@overload
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    by: Callable[[V], Any],
) -> WindowOut[V, V]: ...


@operator
def min_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
    by=_identity,
) -> WindowOut[V, V]:
    """Find the minumum value for each key.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg windower: Window definition.

    :arg by: A function called on each value that is used to extract
        what to compare.

    :returns: Window result streams. Downstream contains the min value
        for each window, once that window has closed.

    """
    return reduce_window("reduce_window", up, clock, windower, partial(min, key=by))


@operator
def reduce_window(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    windower: Windower[Any],
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

    :returns: Window result streams. Downstream contains the reduced
        value for each window, once that window has closed.

    """

    def shim_folder(s: V, v: V) -> V:
        if s is None:
            s = v
        else:
            s = reducer(s, v)

        return s

    return fold_window(
        "fold_window",
        up,
        clock,
        windower,
        _untyped_none,
        shim_folder,
        reducer,
        ordered=False,
    )
