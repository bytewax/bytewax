from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import bytewax.interval as iv
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.interval import IntervalLogic, LeftRight
from bytewax.operators.windowing import EventClock
from bytewax.testing import TestingSink, TestingSource, run_main
from typing_extensions import override


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: str


class _BaseTestLogic(IntervalLogic[_Event, Tuple, Optional[str]]):
    def __init__(self, resume_state: Optional[str]) -> None:
        self.left_value = resume_state

    @override
    def on_value(self, side: LeftRight, value: _Event) -> Iterable[Tuple]:
        if side == "left":
            self.left_value = value.value
            return [("NEW", self.left_value)]
        else:
            return [("RIGHT", self.left_value, value.value)]

    @override
    def on_close(self) -> Iterable[Tuple]:
        return [("CLOSE", self.left_value)]

    @override
    def snapshot(self) -> Optional[str]:
        return self.left_value


def _build_dataflow(
    left_inp: List[_Event],
    right_inp: List[_Event],
    down: List[Tuple],
    unpaired: List[_Event],
    late: List[_Event],
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("left_inp", flow, TestingSource(left_inp))
    rights = op.input("right_inp", flow, TestingSource(right_inp))
    keyed_lefts = op.key_on("key_left", lefts, lambda _x: "KEY")
    keyed_rights = op.key_on("key_right", rights, lambda _x: "KEY")

    def get_ts(e: _Event) -> datetime:
        return e.timestamp

    clock = EventClock(get_ts, wait_for_system_duration=timedelta(seconds=10))
    gap = timedelta(seconds=5)

    int_out = iv.interval(
        "interval",
        keyed_lefts,
        clock,
        gap,
        gap,
        _BaseTestLogic,
        keyed_rights,
    )
    no_keys_down = op.map("no_key_down", int_out.down, lambda x: x[1])
    op.output("out_down", no_keys_down, TestingSink(down))
    no_keys_unpaired = op.map("no_key_unpaired", int_out.unpaired, lambda x: x[1])
    op.output("out_unpaired", no_keys_unpaired, TestingSink(unpaired))
    no_keys_late = op.map("no_key_late", int_out.late, lambda x: x[1])
    op.output("out_late", no_keys_late, TestingSink(late))
    return flow


def test_interval_match() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to, "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("CLOSE", "left1"),
    ]


def test_interval_in_before_gap() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to - timedelta(seconds=3), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("CLOSE", "left1"),
    ]


def test_interval_in_after_gap() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to + timedelta(seconds=3), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("CLOSE", "left1"),
    ]


def test_interval_on_before_gap() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to - timedelta(seconds=5), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("CLOSE", "left1"),
    ]


def test_interval_on_after_gap() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to + timedelta(seconds=5), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("CLOSE", "left1"),
    ]


def test_interval_multi_pair() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
        _Event(align_to + timedelta(seconds=5), "left2"),
    ]
    right_inp = [
        _Event(align_to + timedelta(seconds=3), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("NEW", "left2"),
        ("RIGHT", "left1", "right1"),
        ("RIGHT", "left2", "right1"),
        ("CLOSE", "left1"),
        ("CLOSE", "left2"),
    ]


def test_interval_unpaired() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to + timedelta(seconds=10), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("CLOSE", "left1"),
    ]
    assert unpaired == right_inp


def test_interval_ordered() -> None:
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    left_inp = [
        _Event(align_to, "left1"),
    ]
    right_inp = [
        _Event(align_to + timedelta(seconds=5), "right2"),
        _Event(align_to + timedelta(seconds=3), "right1"),
    ]
    down: List[Tuple[str, str]] = []
    unpaired: List[_Event] = []
    late: List[_Event] = []

    flow = _build_dataflow(left_inp, right_inp, down, unpaired, late)

    run_main(flow)
    assert down == [
        ("NEW", "left1"),
        ("RIGHT", "left1", "right1"),
        ("RIGHT", "left1", "right2"),
        ("CLOSE", "left1"),
    ]
