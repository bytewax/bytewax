from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingBuilderInputConfig
from bytewax.outputs import TestingOutputConfig
from bytewax.window import EventClockConfig, HoppingWindowConfig, TumblingWindowConfig


def test_hopping_window():
    start_at = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    def gen():
        yield ("ALL", {"time": start_at + timedelta(seconds=1), "val": "a"})
        yield ("ALL", {"time": start_at + timedelta(seconds=4), "val": "b"})
        yield ("ALL", {"time": start_at + timedelta(seconds=8), "val": "c"})
        yield ("ALL", {"time": start_at + timedelta(seconds=12), "val": "d"})
        yield ("ALL", {"time": start_at + timedelta(seconds=13), "val": "e"})
        yield ("ALL", {"time": start_at + timedelta(seconds=14), "val": "f"})
        yield ("ALL", {"time": start_at + timedelta(seconds=16), "val": "g"})
        # This is late, and should be ignored
        yield ("ALL", {"time": start_at + timedelta(seconds=1), "val": "h"})

    flow.input("inp", TestingBuilderInputConfig(gen))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = HoppingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at, offset=timedelta(seconds=5)
    )

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", ["a", "b", "c"]),
            ("ALL", ["c", "d", "e", "f"]),
            ("ALL", ["d", "e", "f", "g"]),
            ("ALL", ["g"]),
        ]
    )


def test_tumbling_window():
    start_at = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    def gen():
        yield ("ALL", {"time": start_at, "val": "a"})
        yield ("ALL", {"time": start_at + timedelta(seconds=4), "val": "b"})
        yield ("ALL", {"time": start_at + timedelta(seconds=8), "val": "c"})
        # First 10 second window should close just before processing this item.
        yield ("ALL", {"time": start_at + timedelta(seconds=12), "val": "d"})
        yield ("ALL", {"time": start_at + timedelta(seconds=16), "val": "e"})

    flow.input("inp", TestingBuilderInputConfig(gen))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = TumblingWindowConfig(
        length=timedelta(seconds=10), start_at=start_at
    )

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", ["a", "b", "c"]), ("ALL", ["d", "e"])])
