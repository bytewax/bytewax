from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.operators.window import EventClockConfig, TumblingWindow
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)


def test_collect_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to, "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=8), "user": "a", "val": 1},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
    ]
    out = []

    clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.collect_window("add", clock, windower)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        (
            "a",
            [
                {"time": align_to, "user": "a", "val": 1},
                {"time": align_to + timedelta(seconds=4), "user": "a", "val": 1},
                {"time": align_to + timedelta(seconds=8), "user": "a", "val": 1},
            ],
        ),
        (
            "a",
            [
                {"time": align_to + timedelta(seconds=12), "user": "a", "val": 1},
                {"time": align_to + timedelta(seconds=13), "user": "a", "val": 1},
            ],
        ),
    ]
