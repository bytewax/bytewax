"""Operators for calculating over rolling windows.

**Rolling windows** are different than normal windows in that they
represent a long-lived slice of the timeline of a stream relative to
the current watermark, and items can enter and leave them as time
flows. This lets you express things like "the last ten minutes of
items".

Whenever a value "ages" into or out of the window duration, you can
re-calculate a derived value. You can use this to e.g. calculate a
constantly updating mean of the last 10 minutes of data.

"""

import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Generic, Iterable, List, Optional, Tuple, Union

from typing_extensions import TypeAlias, override

import bytewax.operators as op
from bytewax._utils import partition
from bytewax.dataflow import f_repr, operator
from bytewax.operators import _EMPTY, KeyedStream, S, StatefulBatchLogic, V, W, W_co
from bytewax.operators.windowing import SC, UTC_MIN, ZERO_TD, Clock, ClockLogic


@dataclass(frozen=True)
class _RollingEntry(Generic[V]):
    value: V
    timestamp: datetime


@dataclass(frozen=True)
class _RollingSnapshot(Generic[V, SC, S]):
    clock_snap: SC
    queue: List[_RollingEntry[V]]
    window: List[_RollingEntry[V]]
    state: Optional[S]


@dataclass(frozen=True)
class _Emit(Generic[W]):
    value: W


@dataclass(frozen=True)
class _Late(Generic[V]):
    value: V


_RollingEvent: TypeAlias = Union[_Late[V], _Emit[W]]


@dataclass
class _RollingLogic(
    StatefulBatchLogic[
        V,
        _RollingEvent[V, W],
        _RollingSnapshot[V, SC, S],
    ]
):
    step_id: str
    clock: ClockLogic[V, SC]
    length: timedelta
    offset: timedelta
    mapper: Callable[
        [Optional[S], List[V], List[V], List[V]], Tuple[Optional[S], Iterable[W]]
    ]

    queue: List[_RollingEntry[V]] = field(default_factory=list)
    window: List[_RollingEntry[V]] = field(default_factory=list)
    state: Optional[S] = None

    _last_watermark: datetime = UTC_MIN

    def _is_empty(self) -> bool:
        return len(self.window) <= 0 and len(self.queue) <= 0

    def _add(self, adj_watermark: datetime) -> List[V]:
        add_before = adj_watermark
        # There might be more items at the watermark, so there might
        # be more items at the add boundary, hence `<` not `<=`.
        add_window, self.queue = partition(
            self.queue,
            lambda entry: entry.timestamp < add_before,
        )
        if len(add_window) > 0:
            add_window.sort(key=lambda entry: entry.timestamp)
            # Window will stay sorted because only pre-watermark items
            # that are in timestamp order are added.
            self.window += add_window
        return [entry.value for entry in add_window]

    def _remove(self, adj_watermark: datetime) -> List[V]:
        remove_before = adj_watermark - self.length
        # Length is inclusive so that you can use multiple rolling
        # windows with offset and not get duplicates or missing data.
        # Don't throw something away until its fully after the length.
        remove_window, self.window = partition(
            self.window,
            lambda entry: entry.timestamp < remove_before,
        )
        return [entry.value for entry in remove_window]

    def _process(self, watermark: datetime) -> Iterable[_RollingEvent[V, W]]:
        adj_watermark = watermark - self.offset
        added = self._add(adj_watermark)
        removed = self._remove(adj_watermark)

        # Only call the logic if the rolling window has changed. This
        # is to protect against spurious activations.
        if len(added) > 0 or len(removed) > 0:
            window = [entry.value for entry in self.window]

            res = self.mapper(self.state, window, added, removed)
            try:
                self.state, emit = res
            except TypeError as ex:
                msg = (
                    f"return value of `mapper` {f_repr(self.mapper)} "
                    f"in step {self.step_id!r} "
                    "must be a 2-tuple of `(updated_state, emit_values)`; "
                    f"got a {type(res)!r} instead"
                )
                raise TypeError(msg) from ex

            try:
                return [_Emit(w) for w in emit]
            except TypeError as ex:
                msg = (
                    "second return value `emit_values` "
                    f"in step {self.step_id!r} must be an iterable; "
                    f"got a {type(emit)!r} instead"
                )
                raise TypeError(msg) from ex

        else:
            return _EMPTY

    @override
    def on_batch(self, values: List[V]) -> Tuple[Iterable[_RollingEvent[V, W]], bool]:
        self.clock.before_batch()

        events: List[_RollingEvent[V, W]] = []
        for value in values:
            timestamp, watermark = self.clock.on_item(value)
            assert watermark >= self._last_watermark
            self._last_watermark = watermark

            if timestamp < watermark:
                events.append(_Late(value))
                continue

            entry = _RollingEntry(value, timestamp)
            self.queue.append(entry)

        events += self._process(watermark)

        return (events, self._is_empty())

    @override
    def on_notify(self) -> Tuple[Iterable[_RollingEvent[V, W]], bool]:
        watermark = self.clock.on_notify()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = self._process(watermark)

        return (events, self._is_empty())

    @override
    def on_eof(self) -> Tuple[Iterable[_RollingEvent[V, W]], bool]:
        watermark = self.clock.on_eof()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = self._process(watermark)

        return (events, self._is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        try:
            return self.window[0].timestamp
        except IndexError:
            return None

    @override
    def snapshot(self) -> _RollingSnapshot[V, SC, S]:
        return _RollingSnapshot(
            self.clock.snapshot(),
            list(self.queue),
            list(self.window),
            copy.deepcopy(self.state),
        )


def _unwrap_emit(event: _RollingEvent[V, W]) -> Optional[W]:
    if isinstance(event, _Emit):
        return event.value
    else:
        return None


def _unwrap_late(event: _RollingEvent[V, W]) -> Optional[V]:
    if isinstance(event, _Late):
        return event.value
    else:
        return None


@dataclass(frozen=True)
class RollingOut(Generic[V, W_co]):
    """Streams returned from a rolling window operator."""

    downs: KeyedStream[W_co]
    """Items emitted from this operator."""

    lates: KeyedStream[V]
    """Upstream items that were deemed late to process.

    It's possible you will see window IDs here that were never in the
    `down` or `meta` streams, depending on the specifics of the
    ordering of the data.
    """


@operator
def rolling_delta_stateful_flat_map(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    length: timedelta,
    mapper: Callable[[Optional[S], List[V], List[V]], Tuple[Optional[S], Iterable[W]]],
    offset: timedelta = ZERO_TD,
) -> RollingOut[V, W]:
    """Derive values whenever a rolling window updates incrementally.

    In general you'll want to use the simpler
    {py:obj}`rolling_flat_map` unless re-calculating the entire
    derived value is expensive and you can do incremental updates.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg length: Length of the rolling window, inclusive.

    :arg mapper: Function to be called when the rolling window
        changes; the first argument is any persistent state previously
        returned, a list of `added` values that have just entered the
        window, a list of `removed` values that have just left the
        window. Returns a 2-tuple of `(updated_state, emit_values)`
        with any updated persistent state, and any items to emit
        downstream.

    :arg offset: Queue items for this long before having them enter
        the rolling window. This allows you to setup multiple rolling
        window steps which calculate staggered data e.g. calculate the
        mean of the last hour of data, and the previous hour of data.

    :returns: Rolling result streams.

    """

    def shim_mapper(
        state: Optional[S],
        _window: List[V],
        added: List[V],
        removed: List[V],
    ) -> Tuple[Optional[S], Iterable[W]]:
        return mapper(state, added, removed)

    def shim_builder(
        resume_state: Optional[_RollingSnapshot[V, SC, S]],
    ) -> _RollingLogic[V, W, SC, S]:
        if resume_state is None:
            clock_logic = clock.build(None)
            return _RollingLogic(
                step_id,
                clock_logic,
                length,
                offset,
                shim_mapper,
            )
        else:
            clock_logic = clock.build(resume_state.clock_snap)
            return _RollingLogic(
                step_id,
                clock_logic,
                length,
                offset,
                shim_mapper,
                resume_state.queue,
                resume_state.window,
                resume_state.state,
            )

    events = op.stateful_batch("stateful_batch", up, shim_builder)
    downs: KeyedStream[W] = op.filter_map_value("unwrap_down", events, _unwrap_emit)
    lates: KeyedStream[V] = op.filter_map_value("unwrap_late", events, _unwrap_late)
    return RollingOut(downs, lates)


@operator
def rolling_flat_map(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, Any],
    length: timedelta,
    mapper: Callable[[List[V]], Iterable[W]],
    offset: timedelta = ZERO_TD,
) -> RollingOut[V, W]:
    """Derive updated values whenever a rolling window changes.

    If it's expensive to recalculate a derived data completely, see
    {py:obj}`rolling_delta_stateful_flat_map` which gives you which
    items have been added and removed from the rolling window and
    allows you to incrementally update some state.

    :arg step_id: Unique ID.

    :arg up: Keyed stream.

    :arg clock: Time definition.

    :arg length: Length of the rolling window, inclusive.

    :arg mapper: Function to be called with the current rolling window
        contents whenever a value is added or removed fro the window.
        Returns any items to emit downstream.

    :arg offset: Queue items for this long before having them enter
        the rolling window. This allows you to setup multiple rolling
        window steps which calculate staggered data e.g. calculate the
        mean of the last hour of data, and the previous hour of data.

    :returns: Rolling result streams.

    """

    def shim_mapper(
        state: Optional[None],
        window: List[V],
        added: List[V],
        removed: List[V],
    ) -> Tuple[Optional[None], Iterable[W]]:
        emit = mapper(window)
        return (None, emit)

    def shim_builder(
        resume_state: Optional[_RollingSnapshot[V, SC, None]],
    ) -> _RollingLogic[V, W, SC, None]:
        if resume_state is None:
            clock_logic = clock.build(None)
            return _RollingLogic(
                step_id,
                clock_logic,
                length,
                offset,
                shim_mapper,
            )
        else:
            clock_logic = clock.build(resume_state.clock_snap)
            return _RollingLogic(
                step_id,
                clock_logic,
                length,
                offset,
                shim_mapper,
                resume_state.queue,
                resume_state.window,
                resume_state.state,
            )

    events = op.stateful_batch("stateful_batch", up, shim_builder)
    downs: KeyedStream[W] = op.filter_map_value("unwrap_down", events, _unwrap_emit)
    lates: KeyedStream[V] = op.filter_map_value("unwrap_late", events, _unwrap_late)
    return RollingOut(downs, lates)
