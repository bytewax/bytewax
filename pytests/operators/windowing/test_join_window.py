from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.operators import JoinMode
from bytewax.operators.windowing import EventClock, SessionWindower
from bytewax.testing import TestingSink, TestingSource, run_main


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: int

    def ts_getter(self) -> datetime:
        return self.timestamp


def _build_join_window_dataflow(
    inp_l: List[_Event],
    inp_r: List[_Event],
    out: List[Tuple[Optional[int], Optional[int]]],
    mode: JoinMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _: "ALL")

    clock = EventClock(_Event.ts_getter, wait_for_system_duration=timedelta.max)
    windower = SessionWindower(timedelta(seconds=10))

    joined = win.join_window(
        "join",
        clock,
        windower,
        keyed_lefts,
        keyed_rights,
        mode=mode,
    )
    unkeyed = op.key_rm("unkey", joined.down)

    def clean(
        id_row: Tuple[int, Tuple[Optional[_Event], Optional[_Event]]],
    ) -> Tuple[Optional[int], Optional[int]]:
        _win_id, row = id_row
        v0 = row[0].value if row[0] is not None else None
        v1 = row[1].value if row[1] is not None else None
        return (v0, v1)

    cleaned = op.map("clean", unkeyed, clean)
    op.output("out", cleaned, TestingSink(out))
    return flow


def test_join_window_complete() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        _Event(align_to, 1),
    ]
    inp_r = [
        _Event(align_to + timedelta(seconds=1), 2),
    ]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_window_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_window_final() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        _Event(align_to, 1),
    ]
    inp_r = [
        _Event(align_to + timedelta(seconds=1), 2),
        _Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_window_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        (1, 3),
    ]


def test_join_window_running() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        _Event(align_to, 1),
    ]
    inp_r = [
        _Event(align_to + timedelta(seconds=1), 2),
        _Event(align_to + timedelta(seconds=2), 3),
    ]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_window_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        (1, None),
        (1, 2),
        (1, 3),
    ]


def test_join_window_product() -> None:
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp_l = [
        _Event(align_to, 1),
        _Event(align_to + timedelta(seconds=1), 2),
    ]
    inp_r = [
        _Event(align_to + timedelta(seconds=1), 3),
        _Event(align_to + timedelta(seconds=2), 4),
    ]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_window_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]
