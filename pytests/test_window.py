from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingBuilderInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.window import EventClockConfig, SlidingWindowConfig, TumblingWindowConfig


def test_sliding_window():
    start_at = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    def gen():
        yield ("ALL", {"time": start_at + timedelta(seconds=1), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=4), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=8), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=12), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=13), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=14), "val": 1})
        yield ("ALL", {"time": start_at + timedelta(seconds=16), "val": 1})
        # This is late, and should be ignored
        yield ("ALL", {"time": start_at + timedelta(seconds=1), "val": 10})

    flow.input("inp", TestingBuilderInputConfig(gen))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = SlidingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at, offset=timedelta(seconds=5)
    )

    def add(acc, x):
        return acc + x["val"]

    flow.fold_window("sum", clock_config, window_config, lambda: 0, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", 3), ("ALL", 4), ("ALL", 4), ("ALL", 1)])


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

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
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
