from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.operators import JoinMode
from bytewax.operators.windowing import ZERO_TD, EventClock, SessionWindower
from bytewax.testing import TestingSink, TestingSource, run_main


@dataclass(frozen=True)
class Event:
    timestamp: datetime
    value: int


def build_join_window_dataflow(
    inp_l: List[Event],
    inp_r: List[Event],
    out: List[Tuple[int, int]],
    mode: JoinMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _x: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _x: "ALL")

    clock = EventClock(
        lambda e: e.timestamp, wait_for_system_duration=timedelta(seconds=10)
    )
    windower = SessionWindower(timedelta(seconds=10))

    joined = win.join_window(
        "join",
        clock,
        windower,
        keyed_lefts,
        keyed_rights,
        mode=mode,
    )
    cleaned = op.map(
        "clean",
        joined.down,
        lambda x: tuple(e.value if e is not None else None for e in x[1][1]),
    )
    op.output("out", cleaned, TestingSink(out))
    return flow


def test_join_window_complete() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
    ]
    out: List[Tuple[int, int]] = []

    flow = build_join_window_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_window_final() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
        Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Tuple[int, int]] = []

    flow = build_join_window_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        (1, 3),
    ]


def test_join_window_running() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
        Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Tuple[int, int]] = []

    flow = build_join_window_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        (1, None),
        (1, 2),
        (1, 3),
    ]


def test_join_window_product() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
        Event(align_to + timedelta(seconds=1), 2),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 3),
        Event(align_to + timedelta(seconds=2), 4),
    ]
    out: List[Tuple[int, int]] = []

    flow = build_join_window_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]


def build_join_window_named_dataflow(
    inp_l: List[Event],
    inp_r: List[Event],
    out: List[Dict[str, int]],
    mode: JoinMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _x: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _x: "ALL")

    clock = EventClock(
        lambda e: e.timestamp, wait_for_system_duration=timedelta(seconds=10)
    )
    windower = SessionWindower(timedelta(seconds=1))

    joined = win.join_window_named(
        "join",
        clock,
        windower,
        mode,
        left=keyed_lefts,
        right=keyed_rights,
    )
    cleaned = op.map(
        "clean",
        joined.down,
        lambda x: {side: e.value for side, e in x[1][1].items()},
    )
    op.output("out", cleaned, TestingSink(out))
    return flow


def test_join_window_named_complete() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
    ]
    out: List[Dict[str, int]] = []

    flow = build_join_window_named_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 2},
    ]


def test_join_window_named_final() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
        Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Dict[str, int]] = []

    flow = build_join_window_named_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 3},
    ]


def test_join_window_named_running() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 2),
        Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Dict[str, int]] = []

    flow = build_join_window_named_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        {"left": 1},
        {"left": 1, "right": 2},
        {"left": 1, "right": 3},
    ]


def test_join_window_named_product() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        Event(align_to, 1),
        Event(align_to + timedelta(seconds=1), 2),
    ]
    inp_r = [
        Event(align_to + timedelta(seconds=1), 3),
        Event(align_to + timedelta(seconds=2), 4),
    ]
    out: List[Dict[str, int]] = []

    flow = build_join_window_named_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 3},
        {"left": 1, "right": 4},
        {"left": 2, "right": 3},
        {"left": 2, "right": 4},
    ]
