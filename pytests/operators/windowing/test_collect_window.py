from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import bytewax.operators as op
import bytewax.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.windowing import EventClock, TumblingWindower
from bytewax.testing import TestingSink, TestingSource, run_main


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
