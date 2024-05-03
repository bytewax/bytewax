from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import (
    ZERO_TD,
    EventClock,
    TumblingWindower,
)
from bytewax.testing import TestingSink, TestingSource, run_main


def test_max_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to, "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": 9},
        {"time": align_to + timedelta(seconds=8), "user": "a", "val": 3},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": 10},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": 4},
    ]
    out = []

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    wo = win.max_window("add", s, clock, windower, by=lambda e: e["val"])
    op.output("out", wo.down, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, {"time": align_to + timedelta(seconds=4), "user": "a", "val": 9})),
        ("a", (1, {"time": align_to + timedelta(seconds=12), "user": "a", "val": 10})),
    ]


def test_min_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to, "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": 9},
        {"time": align_to + timedelta(seconds=8), "user": "a", "val": 3},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": 10},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": 4},
    ]
    out = []

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    wo = win.min_window("min", s, clock, windower, by=lambda e: e["val"])
    op.output("out", wo.down, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, {"time": align_to, "user": "a", "val": 1})),
        ("a", (1, {"time": align_to + timedelta(seconds=13), "user": "a", "val": 4})),
    ]
