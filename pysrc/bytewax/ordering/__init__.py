"""Operators related to ordering streams."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Generic, Iterable, List, Literal, Optional, Tuple, Union, cast

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.operators import KeyedStream, StatefulBatchLogic, V
from bytewax.windowing import SC, UTC_MIN, Clock, ClockLogic
from typing_extensions import TypeAlias, override


@dataclass(frozen=True)
class _OrderQueueEntry(Generic[V]):
    value: V
    timestamp: datetime


@dataclass(frozen=True)
class _OrderSnapshot(Generic[V, SC]):
    clock_snap: SC
    queue: List[_OrderQueueEntry[V]]


_Emit: TypeAlias = Tuple[Literal["E"], V]
_Late: TypeAlias = Tuple[Literal["L"], V]
_OrderEvent: TypeAlias = Union[_Emit[V], _Late[V]]


@dataclass
class _OrderLogic(StatefulBatchLogic[V, _OrderEvent[V], _OrderSnapshot[V, SC]]):
    clock: ClockLogic[V, SC]

    queue: List[_OrderQueueEntry[V]] = field(default_factory=list)

    _last_watermark: datetime = UTC_MIN

    def _flush(self, watermark: datetime) -> Iterable[_OrderEvent[V]]:
        still_queued = []

        last_timestamp = UTC_MIN
        for entry in self.queue:
            assert last_timestamp <= entry.timestamp, "queue is not in timestamp order"

            if entry.timestamp <= watermark:
                yield ("E", entry.value)
            else:
                still_queued.append(entry)

        self.queue = still_queued

    def _is_empty(self) -> bool:
        return len(self.queue) <= 0

    @override
    def on_batch(self, values: List[V]) -> Tuple[Iterable[_OrderEvent[V]], bool]:
        self.clock.before_batch()

        events: List[_OrderEvent[V]] = []
        for value in values:
            timestamp, watermark = self.clock.on_item(value)
            assert watermark >= self._last_watermark
            self._last_watermark = watermark

            if timestamp < watermark:
                events.append(("L", value))
                continue

            entry = _OrderQueueEntry(value, timestamp)
            self.queue.append(entry)

        self.queue.sort(key=lambda entry: entry.timestamp)
        events.extend(self._flush(watermark))

        return (events, self._is_empty())

    @override
    def on_notify(self) -> Tuple[Iterable[_OrderEvent[V]], bool]:
        watermark = self.clock.on_notify()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = list(self._flush(watermark))

        return (events, self._is_empty())

    @override
    def on_eof(self) -> Tuple[Iterable[_OrderEvent[V]], bool]:
        watermark = self.clock.on_eof()
        assert watermark >= self._last_watermark
        self._last_watermark = watermark

        events = list(self._flush(watermark))

        return (events, self._is_empty())

    @override
    def notify_at(self) -> Optional[datetime]:
        notify_at = min((entry.timestamp for entry in self.queue), default=None)
        if notify_at is not None:
            notify_at = self.clock.to_system_utc(notify_at)
        return notify_at

    @override
    def snapshot(self) -> _OrderSnapshot[V, SC]:
        return _OrderSnapshot(
            self.clock.snapshot(),
            list(self.queue),
        )


@dataclass(frozen=True)
class OrderOut(Generic[V]):
    """Streams returned from the `order` operator."""

    down: KeyedStream[V]
    """Items in timestamp order."""
    late: KeyedStream[V]
    """Upstream items that were late.


    """


def _unwrap_order_emit(event: _OrderEvent[V]) -> Optional[V]:
    typ, obj = event
    if typ == "E":
        value = cast(V, obj)
        return value
    else:
        return None


def _unwrap_order_late(event: _OrderEvent[V]) -> Optional[V]:
    typ, obj = event
    if typ == "L":
        value = cast(V, obj)
        return value
    else:
        return None


@operator
def order(
    step_id: str,
    up: KeyedStream[V],
    clock: Clock[V, SC],
) -> OrderOut[V]:
    """Order items by timestamp, within limits.

    This allows you to take a stream of out-of-timestamp-order items
    and streaming sort them into timestamp order. Further stateful
    processing (e.g. a state machine) might need items in timestamp
    order.

    You generally will use this operator with an
    {py:obj}`~bytewax.operators.windowing.EventClock`.
    {py:obj}`~bytewax.operators.windowing.EventClock.wait_for_system_duration`
    is a crucial parameter to think about for this clock: The larger
    this value is, the more out-of-order the timestamps can be in your
    upstream without dropping items as late. But the tradeoff is that
    this increases the memory usage (as all items for that duration
    need to be buffered) and increases the latency of a real-time data
    source (in that you will need to wait this amount of system time
    to see if new out-of-order data is still coming).

    For a streaming system, generally set
    {py:obj}`~bytewax.operators.windowing.EventClock.wait_for_system_duration`
    to the amount of system time latency you are willing to wait for
    as correct answers as possible.

    For bounded batch processing, generally set
    {py:obj}`~bytewax.operators.windowing.EventClock.wait_for_system_duration`
    to the maximum amount of out-of-orderedness you think you'll see
    in the data. You also can set
    {py:obj}`~bytewax.operators.windowing.EventClock.wait_for_system_duration`
    to {py:obj}`datetime.timedelta.max` if you are ok with buffering all of the
    incoming data and sorting it and don't want to worry about a
    maximum amount of out-of-order-ness, but this will have memory
    usage implications.

    This operator _is not needed_ before standard
    {py:obj}`~bytewax.operators.windowing` operators; they sort
    internally to give you their operator guarantees.

    :arg step_id: Unique ID.

    :arg up: Stream.

    :arg clock: Time definition. This will almost always be an
        {py:obj}`~bytewax.operators.windowing.EventClock`.

    :returns: Order result streams.

    """

    def shim_builder(
        resume_state: Optional[_OrderSnapshot[V, SC]],
    ) -> _OrderLogic[V, SC]:
        if resume_state is not None:
            clock_logic = clock.build(resume_state.clock_snap)
            return _OrderLogic(clock_logic, resume_state.queue)
        else:
            clock_logic = clock.build(None)
            return _OrderLogic(clock_logic)

    events = op.stateful_batch("stateful_batch", up, shim_builder)
    downs: KeyedStream[V] = op.filter_map_value(
        "unwrap_down",
        events,
        _unwrap_order_emit,
    )
    lates: KeyedStream[V] = op.filter_map_value(
        "unwrap_late",
        events,
        _unwrap_order_late,
    )
    return OrderOut(downs, lates)
