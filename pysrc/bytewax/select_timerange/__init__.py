"""Operators to query built-up state.

These operators usually have two inputs, one is a stream of updated
data; the other is a "query stream" which contains how to query that
data. Downstream will be the result of each query.

"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    Any,
    Generic,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import bytewax.operators as op
from bytewax._utils import partition
from bytewax.dataflow import operator
from bytewax.operators import KeyedStream, StatefulBatchLogic, V
from bytewax.operators.windowing import SC, UTC_MIN, Clock, ClockLogic
from typing_extensions import assert_never, override


TC = TypeVar("TC")
"""Type of secondary {py:obj}`bytewax.operators.windowing.ClockLogic`'s
state snapshots.
"""


@dataclass(frozen=True)
class TimeRangeQuery:
    """A query to select values in a time range."""

    sel_id: str
    """Arbitrary ID to identify this query in results."""

    lower: datetime
    """Earlier bound.

    The results will be inclusive of this time.
    """

    upper: datetime
    """Later bound.

    The results will be inclusive of this time.
    """


@dataclass(frozen=True)
class TimeRangeResult(Generic[V]):
    """Results from selecting a time range."""

    query: TimeRangeQuery

    values: List[V]

    incomplete: bool = False
    """`True` if some values are missing from the query because the
    operator was configured with too short of a buffer via
    {py:obj}`~select_timerange`.

    """


@dataclass(frozen=True)
class _UpValue(Generic[V]):
    value: V


@dataclass(frozen=True)
class _UpQuery:
    query: TimeRangeQuery


@dataclass(frozen=True)
class _DownResult(Generic[V]):
    result: TimeRangeResult[V]


@dataclass(frozen=True)
class _DownLateValue(Generic[V]):
    value: V


@dataclass(frozen=True)
class _DownLateQuery:
    query: TimeRangeQuery


@dataclass(frozen=True)
class _SelectTimeRangeEntry(Generic[V]):
    value: V
    timestamp: datetime


@dataclass(frozen=True)
class _SelectTimeRangeSnapshot(Generic[V, SC, TC]):
    up_clock_snap: SC
    query_clock_snap: TC
    up_queue: List[_SelectTimeRangeEntry[V]]
    query_queue: List[TimeRangeQuery]


@dataclass
class _SelectTimeRangeLogic(
    StatefulBatchLogic[
        Union[_UpValue[V], _UpQuery],
        Union[_DownResult[V], _DownLateValue[V], _DownLateQuery],
        _SelectTimeRangeSnapshot[V, SC, TC],
    ]
):
    up_clock: ClockLogic[V, SC]
    query_clock: ClockLogic[TimeRangeQuery, TC]
    max_query_length: timedelta

    up_queue: List[_SelectTimeRangeEntry[V]] = field(default_factory=list)
    query_queue: List[TimeRangeQuery] = field(default_factory=list)

    _last_up_watermark: datetime = UTC_MIN
    _last_query_watermark: datetime = UTC_MIN

    def _is_empty(self) -> bool:
        return len(self.up_queue) + len(self.query_queue) <= 0

    def _flush(
        self,
    ) -> Iterable[Union[_DownResult[V], _DownLateValue[V], _DownLateQuery]]:
        watermark = min(self._last_up_watermark, self._last_query_watermark)

        due_queries, still_query_queue = partition(
            self.query_queue, lambda s: s.upper <= watermark
        )

        for query in due_queries:
            duration = query.upper - query.lower

            values = [
                e.value
                for e in self.up_queue
                if e.timestamp >= query.lower and e.timestamp <= query.upper
            ]
            incomplete = duration > self.max_query_length
            yield _DownResult(TimeRangeResult(query, values, incomplete))

        try:
            retain_after = watermark - self.max_query_length
        except OverflowError:
            retain_after = UTC_MIN
        self.up_queue = [e for e in self.up_queue if e.timestamp >= retain_after]
        self.query_queue = still_query_queue

    @override
    def on_batch(
        self, values: List[Union[_UpValue[V], _UpQuery]]
    ) -> Tuple[
        Iterable[Union[_DownResult[V], _DownLateValue[V], _DownLateQuery]],
        bool,
    ]:
        self.up_clock.before_batch()
        self.query_clock.before_batch()

        emit: List[Union[_DownResult, _DownLateValue, _DownLateQuery]] = []

        for value in values:
            if isinstance(value, _UpValue):
                up_value = value.value

                value_timestamp, watermark = self.up_clock.on_item(up_value)
                assert watermark >= self._last_up_watermark
                self._last_up_watermark = watermark

                if value_timestamp < watermark:
                    emit.append(_DownLateValue(value.value))
                    continue

                entry = _SelectTimeRangeEntry(up_value, value_timestamp)
                self.up_queue.append(entry)

            elif isinstance(value, _UpQuery):
                query = value.query

                value_timestamp, watermark = self.query_clock.on_item(query)
                assert watermark >= self._last_query_watermark
                self._last_query_watermark = watermark

                assert value_timestamp == query.upper, (
                    "`query_clock` must be an `EventClock` "
                    "which returns the `upper` bound of each query"
                )
                if value_timestamp < watermark:
                    emit.append(_DownLateQuery(query))
                    continue

                self.query_queue.append(query)

            else:
                assert_never(value)

        self.up_queue.sort(key=lambda e: e.timestamp)
        self.query_queue.sort(key=lambda s: s.upper)
        emit.extend(self._flush())

        return (emit, self._is_empty())

    @override
    def on_notify(
        self,
    ) -> Tuple[
        Iterable[Union[_DownResult[V], _DownLateValue[V], _DownLateQuery]],
        bool,
    ]:
        watermark = self.up_clock.on_notify()
        assert watermark >= self._last_up_watermark
        self._last_up_watermark = watermark
        watermark = self.query_clock.on_notify()
        assert watermark >= self._last_query_watermark
        self._last_query_watermark = watermark

        emit = list(self._flush())

        return (emit, self._is_empty())

    @override
    def on_eof(
        self,
    ) -> Tuple[
        Iterable[Union[_DownResult[V], _DownLateValue[V], _DownLateQuery]],
        bool,
    ]:
        watermark = self.up_clock.on_eof()
        assert watermark >= self._last_up_watermark
        self._last_up_watermark = watermark
        watermark = self.query_clock.on_eof()
        assert watermark >= self._last_query_watermark
        self._last_query_watermark = watermark

        emit = list(self._flush())

        return (emit, self._is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        watermark = min(self._last_up_watermark, self._last_query_watermark)
        return min(
            (query.upper for query in self.query_queue if query.upper > watermark),
            default=None,
        )

    @override
    def snapshot(self) -> _SelectTimeRangeSnapshot[V, SC, TC]:
        return _SelectTimeRangeSnapshot(
            self.up_clock.snapshot(),
            self.query_clock.snapshot(),
            list(self.up_queue),
            list(self.query_queue),
        )


def time_range_ts_getter(query: TimeRangeQuery) -> datetime:
    """Timestamp getter for the select query stream."""
    return query.upper


@dataclass(frozen=True)
class SelectTimeRangeOut(Generic[V]):
    """Streams returned from the select time range operator."""

    results: KeyedStream[TimeRangeResult[V]]
    """Selection results."""

    late_ups: KeyedStream[V]
    """Upstream items that were deemed late."""

    late_queries: KeyedStream[TimeRangeQuery]
    """Upstream queries that were deemed late."""


def _unwrap_down(
    value: Union[_DownResult[V], _DownLateValue[V], _DownLateQuery],
) -> Optional[TimeRangeResult[V]]:
    if isinstance(value, _DownResult):
        return value.result
    else:
        return None


def _unwrap_late_value(
    value: Union[_DownResult[V], _DownLateValue[V], _DownLateQuery],
) -> Optional[V]:
    if isinstance(value, _DownLateValue):
        return value.value
    else:
        return None


def _unwrap_late_query(
    value: Union[_DownResult[V], _DownLateValue[V], _DownLateQuery],
) -> Optional[TimeRangeQuery]:
    if isinstance(value, _DownLateQuery):
        return value.query
    else:
        return None


@operator
def select_time_range(
    step_id: str,
    up: KeyedStream[V],
    up_clock: Clock[V, Any],
    queries: KeyedStream[TimeRangeQuery],
    query_clock: Clock[TimeRangeQuery, Any],
    max_query_length: timedelta,
) -> SelectTimeRangeOut[V]:
    """Select ranges of buffered timestamped values.

    :arg step_id: Unique ID.

    :arg up: Keyed stream of values to buffer.

    :arg up_clock: Time definition.

    :arg queries: Keyed stream of time ranges to select.

    :arg query_clock: This should be an
        {py:obj}`~bytewax.operators.windowing.EventClock` where
        {py:obj}`~bytewax.operators.windowing.EventClock.ts_getter` is
        {py:obj}`time_range_ts_getter`.

    :arg max_query_length: Buffer size. Incoming queries that request
        more than this will be missing values from their results and
        tagged as {py:obj}`TimeRangeResult.incomplete`.

    :returns:
        A {py:obj}`SelectTimeRangeOut` dataclass.

    """
    wrapped_up = op.map_value("wrap_up", up, _UpValue)
    wrapped_queries = op.map_value("wrap_queries", queries, _UpQuery)
    merged: KeyedStream[Union[_UpValue, _UpQuery]] = op.merge(
        "merge", wrapped_up, wrapped_queries
    )

    def shim_builder(
        resume_state: Optional[_SelectTimeRangeSnapshot[V, SC, TC]],
    ) -> _SelectTimeRangeLogic[V, SC, TC]:
        if resume_state is not None:
            up_clock_logic = up_clock.build(resume_state.up_clock_snap)
            query_clock_logic = query_clock.build(resume_state.query_clock_snap)
            return _SelectTimeRangeLogic(
                up_clock_logic,
                query_clock_logic,
                max_query_length,
                resume_state.up_queue,
                resume_state.query_queue,
            )
        else:
            up_clock_logic = up_clock.build(None)
            query_clock_logic = query_clock.build(None)
            return _SelectTimeRangeLogic(
                up_clock_logic,
                query_clock_logic,
                max_query_length,
            )

    events = op.stateful_batch("stateful_batch", merged, shim_builder)

    results: KeyedStream[TimeRangeResult[V]] = op.filter_map_value(
        "split_type_value_down",
        events,
        _unwrap_down,
    )
    late_ups: KeyedStream[V] = op.filter_map_value(
        "split_type_value_late_up",
        events,
        _unwrap_late_value,
    )
    late_queries: KeyedStream[TimeRangeQuery] = op.filter_map_value(
        "split_type_value_late_query",
        events,
        _unwrap_late_query,
    )
    return SelectTimeRangeOut(results, late_ups, late_queries)
