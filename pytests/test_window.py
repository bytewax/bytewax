from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingBuilderInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.window import TestingClockConfig, TumblingWindowConfig


def test_tumbling_window():
    start_at = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    def gen():
        yield ("ALL", {"time": start_at, "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=4), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=8), "val": 1})
        # First 10 second window should close just before processing this item.
        yield ("ALL", {"time": start_at + timedelta(seconds=12), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=16), "val": 1})

    flow.input("inp", TestingBuilderInputConfig(gen))

    clock_config = TestingClockConfig(lambda e: e["time"], wait_until_end=True)
    window_config = TumblingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at
    )

    def add(acc, x):
        if type(acc) == dict:
            return acc["val"] + x["val"]
        else:
            return acc + x["val"]

    flow.reduce_window("sum", clock_config, window_config, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", 3), ("ALL", 2)])
