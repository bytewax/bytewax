from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.dataflow import Dataflow
from bytewax.operators.window import EventClockConfig, TumblingWindow, WindowMetadata
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)
ALIGN_TO = datetime(2022, 1, 1, tzinfo=timezone.utc)


def after(seconds):
    return ALIGN_TO + timedelta(seconds=seconds)


def test_count_window():
    inp = [
        {"time": after(seconds=0), "user": "a", "val": 1},
        {"time": after(seconds=4), "user": "a", "val": 1},
        {"time": after(seconds=8), "user": "b", "val": 1},
        {"time": after(seconds=12), "user": "a", "val": 1},
        {"time": after(seconds=13), "user": "a", "val": 1},
    ]
    out = []

    clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindow(length=timedelta(seconds=10), align_to=ALIGN_TO)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = win.count_window("add", s, clock, windower, lambda e: e["user"])
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == [
        ("a", (WindowMetadata(ALIGN_TO, after(10)), 2)),
        ("a", (WindowMetadata(after(10), after(20)), 2)),
        ("b", (WindowMetadata(ALIGN_TO, after(10)), 1)),
    ]
