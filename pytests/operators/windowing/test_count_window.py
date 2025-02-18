from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.windowing import ZERO_TD, EventClock, TumblingWindower
from bytewax.testing import TestingSink, TestingSource, run_main


def test_count_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to + timedelta(seconds=0), "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=8), "user": "b", "val": 1},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
    ]
    out = []

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    wo = win.count_window("add", s, clock, windower, lambda e: e["user"])
    op.output("out", wo.down, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, 2)),
        ("a", (1, 2)),
        ("b", (0, 1)),
    ]
