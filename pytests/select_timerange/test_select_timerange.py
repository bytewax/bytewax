from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream
from bytewax.select_timerange import (
    TimeRangeQuery,
    TimeRangeResult,
    select_time_range,
    time_range_ts_getter,
)
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.windowing import EventClock


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: str


def test_select_time_range() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    event_a = _Event(align_to, "a")
    event_b = _Event(align_to + timedelta(seconds=1), "b")
    event_c = _Event(align_to + timedelta(seconds=2), "c")
    event_d = _Event(align_to + timedelta(seconds=3), "d")
    event_e = _Event(align_to + timedelta(seconds=4), "e")
    values_inp = [
        event_a,
        event_b,
        event_c,
        event_d,
        event_e,
    ]
    query_x = TimeRangeQuery(
        "x",
        align_to,
        align_to + timedelta(seconds=2),
    )
    query_y = TimeRangeQuery(
        "y",
        align_to + timedelta(seconds=1),
        align_to + timedelta(seconds=1),
    )
    query_inp = [
        query_x,
        query_y,
    ]
    out: List[Tuple[int, List[int]]] = []

    flow = Dataflow("test_df")
    values = op.input("inp_values", flow, TestingSource(values_inp))
    queries = op.input("inp_queries", flow, TestingSource(query_inp))
    keyed_values = op.key_on("key_values", values, lambda _: "ALL")
    keyed_queries = op.key_on("key_queries", queries, lambda _: "ALL")

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    value_clock = EventClock(ts_getter, wait_for_system_duration=timedelta.max)
    query_clock = EventClock(
        time_range_ts_getter, wait_for_system_duration=timedelta.max
    )

    select_out = select_time_range(
        "select_time_range",
        keyed_values,
        value_clock,
        keyed_queries,
        query_clock,
        max_query_length=timedelta(seconds=10),
    )

    cleaned: Stream = op.key_rm("unkey", select_out.results)
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)
    assert out == [
        TimeRangeResult(query_y, [event_b], False),
        TimeRangeResult(query_x, [event_a, event_b, event_c], False),
    ]


def test_select_time_range_incomplete() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    event = _Event(align_to, "a")
    values_inp = [
        event,
    ]
    query = TimeRangeQuery(
        "x",
        align_to,
        align_to + timedelta(seconds=30),
    )
    query_inp = [
        query,
    ]
    out: List[Tuple[int, List[int]]] = []

    flow = Dataflow("test_df")
    values = op.input("inp_values", flow, TestingSource(values_inp))
    queries = op.input("inp_queries", flow, TestingSource(query_inp))
    keyed_values = op.key_on("key_values", values, lambda _: "ALL")
    keyed_queries = op.key_on("key_queries", queries, lambda _: "ALL")

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    value_clock = EventClock(ts_getter, wait_for_system_duration=timedelta.max)
    query_clock = EventClock(
        time_range_ts_getter, wait_for_system_duration=timedelta.max
    )

    select_out = select_time_range(
        "select_time_range",
        keyed_values,
        value_clock,
        keyed_queries,
        query_clock,
        max_query_length=timedelta(seconds=10),
    )

    cleaned: Stream = op.key_rm("unkey", select_out.results)
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)
    assert out == [
        TimeRangeResult(query, [event], True),
    ]
