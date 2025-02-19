from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import bytewax.operators as op
import bytewax.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.windowing import (
    ZERO_TD,
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
)
from pytest import mark

# TODO: Test snapshotting logic so we're sure a recovery roundtrip
# would work.


@dataclass(frozen=True)
class _UserEvent:
    timestamp: datetime
    typ: str


def _merge_defaultdict(
    a: Dict[str, int],
    b: Dict[str, int],
) -> Dict[str, int]:
    a.update(b)
    return a


def test_fold_window_tumbling() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        _UserEvent(align_to, "login"),
        _UserEvent(align_to + timedelta(seconds=4), "post"),
        _UserEvent(align_to + timedelta(seconds=8), "post"),
        # First 10 sec window closes during processing this input.
        _UserEvent(align_to + timedelta(seconds=16), "post"),
    ]
    out: List[Tuple[int, Dict[str, int]]] = []

    flow = Dataflow("test_df")
    events = op.input("inp", flow, TestingSource(inp))
    keyed_events = op.key_on("key", events, lambda _: "ALL")

    def ts_getter(event: _UserEvent) -> datetime:
        return event.timestamp

    clock = EventClock(ts_getter, wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    def builder() -> Dict[str, int]:
        return defaultdict(int)

    def count(counts: Dict[str, int], event: _UserEvent) -> Dict[str, int]:
        typ = event.typ
        counts[typ] += 1
        return counts

    fold_out = win.fold_window(
        "count",
        keyed_events,
        clock,
        windower,
        builder,
        count,
        _merge_defaultdict,
    )
    unkeyed = op.key_rm("key_rm", fold_out.down)

    def map_dict(id_value: Tuple[int, Dict[str, int]]) -> Tuple[int, Dict[str, int]]:
        win_id, value = id_value
        return (win_id, dict(value))

    cleaned = op.map("normal_dict", unkeyed, map_dict)
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)
    assert out == [
        (0, {"login": 1, "post": 2}),
        (1, {"post": 1}),
    ]


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: str


def test_fold_window_session() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        # Session 1
        _Event(align_to + timedelta(seconds=1), "a"),
        _Event(align_to + timedelta(seconds=5), "b"),
        # Session 2
        _Event(align_to + timedelta(seconds=11), "c"),
        _Event(align_to + timedelta(seconds=12), "d"),
        _Event(align_to + timedelta(seconds=13), "e"),
        _Event(align_to + timedelta(seconds=14), "f"),
        # Session 3
        _Event(align_to + timedelta(seconds=20), "g"),
        # This is late, and should be ignored
        _Event(align_to + timedelta(seconds=1), "h"),
    ]
    out: List[Tuple[int, List[str]]] = []

    flow = Dataflow("test_df")
    events = op.input("inp", flow, TestingSource(inp))
    keyed_events = op.key_on("key", events, lambda _: "ALL")

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    clock = EventClock(ts_getter, wait_for_system_duration=ZERO_TD)
    windower = SessionWindower(gap=timedelta(seconds=5))

    def add(acc: List[str], event: _Event) -> List[str]:
        acc.append(event.value)
        return acc

    fold_out = win.fold_window(
        "sum", keyed_events, clock, windower, list, add, list.__add__
    )
    unkeyed = op.key_rm("unkey", fold_out.down)
    op.output("out", unkeyed, TestingSink(out))

    run_main(flow)
    assert out == [
        (0, ["a", "b"]),
        (1, ["c", "d", "e", "f"]),
        (2, ["g"]),
    ]


def test_fold_window_sliding() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    # Valign_to
    #  a  b   c   def g
    #  h
    # -----)
    # [---------)
    #      [---------)
    #           [---------)
    #                [---------)
    inp = [
        _Event(align_to + timedelta(seconds=1), "a"),
        _Event(align_to + timedelta(seconds=4), "b"),
        _Event(align_to + timedelta(seconds=8), "c"),
        _Event(align_to + timedelta(seconds=12), "d"),
        _Event(align_to + timedelta(seconds=13), "e"),
        _Event(align_to + timedelta(seconds=14), "f"),
        _Event(align_to + timedelta(seconds=16), "g"),
        # This is late, and should be ignored.
        _Event(align_to + timedelta(seconds=1), "h"),
    ]
    out: List[Tuple[int, List[str]]] = []

    flow = Dataflow("test_df")
    events = op.input("inp", flow, TestingSource(inp))
    keyed_events = op.key_on("key", events, lambda _: "ALL")

    def ts_getter(event: _Event) -> datetime:
        return event.timestamp

    clock = EventClock(ts_getter, wait_for_system_duration=ZERO_TD)
    windower = SlidingWindower(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=align_to,
    )

    def add(acc: List[str], event: _Event) -> List[str]:
        acc.append(event.value)
        return acc

    fold_out = win.fold_window(
        "sum", keyed_events, clock, windower, list, add, list.__add__
    )
    unkeyed = op.key_rm("unkey", fold_out.down)
    op.output("out", unkeyed, TestingSink(out))

    run_main(flow)
    assert out == [
        (-1, ["a", "b"]),
        (0, ["a", "b", "c"]),
        (1, ["c", "d", "e", "f"]),
        (2, ["d", "e", "f", "g"]),
        (3, ["g"]),
    ]


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_fold_window_benchmark(benchmark, entry_point) -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)

    inp = [align_to + timedelta(seconds=i) for i in range(100_000)]
    out: List[Tuple[int, None]] = []

    flow = Dataflow("bench")
    times = op.input("in", flow, TestingSource(inp, 10))
    keyed_times = op.key_on("key", times, lambda _: "ALL")

    clock = EventClock(lambda x: x, wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(timedelta(minutes=1), align_to)

    fold_out = win.fold_window(
        "fold_window",
        keyed_times,
        clock,
        windower,
        lambda: None,
        lambda s, _: s,
        lambda s, _: s,
        ordered=False,
    )

    unkeyed = op.key_rm("unkey", fold_out.down)
    op.output("out", unkeyed, TestingSink(out))

    expected = [(i, None) for i in range(1667)]

    def run():
        entry_point(flow)
        assert out == expected
        out.clear()

    benchmark(run)
