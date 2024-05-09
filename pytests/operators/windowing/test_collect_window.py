from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, TumblingWindower
from bytewax.testing import TestingSink, TestingSource, run_main


def test_collect_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to, "user": "a", "val": 1},
        {"time": align_to + timedelta(seconds=8), "user": "a", "val": 3},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": 2},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": 5},
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": 4},
    ]
    out = []

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=timedelta.max)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

    flow = Dataflow("test_df")
    inps = op.input("inp", flow, TestingSource(inp))
    keyed_inps = op.key_on("key_inp", inps, lambda e: e["user"])
    wo = win.collect_window("collect_window", keyed_inps, clock, windower)

    def clean(id_collected: Tuple[int, List[Dict]]) -> Tuple[int, List[int]]:
        window_id, collected = id_collected
        cleaned = [event["val"] for event in collected]
        return (window_id, cleaned)

    cleans = op.map_value("clean", wo.down, clean)
    op.output("out", cleans, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, [1, 2, 3])),
        ("a", (1, [4, 5])),
    ]
