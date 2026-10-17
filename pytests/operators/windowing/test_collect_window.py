from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.operators.windowing import EventClock, TumblingWindower
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: int


def test_collect_window() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        _Event(align_to, 1),
        _Event(align_to + timedelta(seconds=8), 3),
        _Event(align_to + timedelta(seconds=4), 2),
        # First 10 sec window closes during processing this input.
        _Event(align_to + timedelta(seconds=13), 5),
        _Event(align_to + timedelta(seconds=12), 4),
    ]
    out: List[Tuple[int, List[int]]] = []

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    clock = EventClock(ts_getter, wait_for_system_duration=timedelta.max)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed_inps = op.key_on("key", inps, lambda _: "ALL")
    collect_out = win.collect_window("collect_window", keyed_inps, clock, windower)
    unkeyed = op.key_rm("unkey", collect_out.down)

    def clean(
        id_collected: Tuple[int, List[_Event]],
    ) -> Tuple[int, List[int]]:
        window_id, collected = id_collected
        cleaned = [event.value for event in collected]
        return (window_id, cleaned)

    cleans = op.map("clean", unkeyed, clean)
    op.output("out", cleans, TestingSink(out))

    run_main(flow)
    assert out == [
        (0, [1, 2, 3]),
        (1, [4, 5]),
    ]


def test_collect_window_unordered() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        _Event(align_to, 1),
        _Event(align_to + timedelta(seconds=8), 3),
        _Event(align_to + timedelta(seconds=4), 2),
        _Event(align_to + timedelta(seconds=13), 5),
        _Event(align_to + timedelta(seconds=12), 4),
    ]
    out: List[Tuple[int, List[int]]] = []

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    clock = EventClock(ts_getter, wait_for_system_duration=timedelta.max)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed_inps = op.key_on("key", inps, lambda _: "ALL")
    collect_out = win.collect_window(
        "collect_window", keyed_inps, clock, windower, ordered=False
    )
    unkeyed = op.key_rm("unkey", collect_out.down)

    def clean(
        id_collected: Tuple[int, List[_Event]],
    ) -> Tuple[int, List[int]]:
        window_id, collected = id_collected
        cleaned = [event.value for event in collected]
        return (window_id, cleaned)

    cleans = op.map("clean", unkeyed, clean)
    op.output("out", cleans, TestingSink(out))

    run_main(flow)
    # Items are collected in arrival order, not timestamp order.
    assert out == [
        (0, [1, 3, 2]),
        (1, [5, 4]),
    ]


def test_collect_window_into_set() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        _Event(align_to, 1),
        _Event(align_to + timedelta(seconds=8), 3),
        # Duplicates collapse.
        _Event(align_to, 1),
        _Event(align_to + timedelta(seconds=13), 5),
        _Event(align_to + timedelta(seconds=13), 5),
    ]
    out: List[Tuple[int, Set[int]]] = []

    clock: EventClock[_Event] = EventClock(
        lambda e: e.timestamp, wait_for_system_duration=timedelta.max
    )
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", inps, lambda _: "ALL")
    collect_out = win.collect_window("collect_window", keyed, clock, windower, set)

    def values(id_events: Tuple[int, Set[_Event]]) -> Tuple[int, Set[int]]:
        window_id, events = id_events
        return (window_id, {e.value for e in events})

    unkeyed = op.key_rm("unkey", collect_out.down)
    op.output("out", op.map("values", unkeyed, values), TestingSink(out))

    run_main(flow)
    # Only the duplicates collapsed: each window got one event per value.
    assert out == [(0, {1, 3}), (1, {5})]


def test_collect_window_into_dict() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        ("a", _Event(align_to, 1)),
        ("b", _Event(align_to + timedelta(seconds=8), 2)),
        # Earlier than the `b` above, so the `b` above wins.
        ("b", _Event(align_to + timedelta(seconds=4), 3)),
        ("a", _Event(align_to + timedelta(seconds=13), 4)),
    ]
    out: List[Tuple[int, Dict[str, int]]] = []

    clock: EventClock[Tuple[str, _Event]] = EventClock(
        lambda k_e: k_e[1].timestamp, wait_for_system_duration=timedelta.max
    )
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", inps, lambda _: "ALL")
    collect_out = win.collect_window("collect_window", keyed, clock, windower, dict)

    def values(
        id_d: Tuple[int, Dict[str, _Event]],
    ) -> Tuple[int, Dict[str, int]]:
        window_id, d = id_d
        return (window_id, {k: e.value for k, e in d.items()})

    unkeyed = op.key_rm("unkey", collect_out.down)
    op.output("out", op.map("values", unkeyed, values), TestingSink(out))

    run_main(flow)
    assert out == [(0, {"a": 1, "b": 2}), (1, {"a": 4})]


def test_collect_window_into_dict_raises_on_non_pair() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [_Event(align_to, 1)]

    clock: EventClock[_Event] = EventClock(
        lambda e: e.timestamp, wait_for_system_duration=timedelta.max
    )
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", inps, lambda _: "ALL")
    collect_out = win.collect_window("collect_window", keyed, clock, windower, dict)
    op.output("out", collect_out.down, TestingSink([]))

    with raises(BytewaxRuntimeError) as exc_info:
        run_main(flow)
    causes: List[BaseException] = []
    ex: Optional[BaseException] = exc_info.value
    while ex is not None:
        causes.append(ex)
        ex = ex.__cause__
    assert any(
        isinstance(c, TypeError) and "requires `(key, value)` 2-tuple" in str(c)
        for c in causes
    )
