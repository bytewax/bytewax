from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput
from bytewax.outputs import TestingOutputConfig
from bytewax.window import (
    EventClockConfig,
    HoppingWindowConfig,
    TumblingWindowConfig,
    SessionWindow,
)


class Dt(datetime):
    def after(self, s):
        return self + timedelta(seconds=s)


def test_session_window():
    start_at = Dt(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    inp = [
        # Session 1
        ("ALL", {"time": start_at.after(1), "val": "a"}),
        ("ALL", {"time": start_at.after(5), "val": "b"}),
        # Session 2
        ("ALL", {"time": start_at.after(11), "val": "c"}),
        ("ALL", {"time": start_at.after(12), "val": "d"}),
        ("ALL", {"time": start_at.after(13), "val": "e"}),
        ("ALL", {"time": start_at.after(14), "val": "f"}),
        # Session 3
        ("ALL", {"time": start_at.after(20), "val": "g"}),
        # This is late, and should be ignored
        ("ALL", {"time": start_at.after(1), "val": "h"}),
    ]

    flow.input("inp", TestingInput(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = SessionWindow(gap=timedelta(seconds=5))

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", ["a", "b"]),
            ("ALL", ["c", "d", "e", "f"]),
            ("ALL", ["g"]),
        ]
    )


def test_hopping_window():
    start_at = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    inp = [
        ("ALL", {"time": start_at + timedelta(seconds=1), "val": "a"}),
        ("ALL", {"time": start_at + timedelta(seconds=4), "val": "b"}),
        ("ALL", {"time": start_at + timedelta(seconds=8), "val": "c"}),
        ("ALL", {"time": start_at + timedelta(seconds=12), "val": "d"}),
        ("ALL", {"time": start_at + timedelta(seconds=13), "val": "e"}),
        ("ALL", {"time": start_at + timedelta(seconds=14), "val": "f"}),
        ("ALL", {"time": start_at + timedelta(seconds=16), "val": "g"}),
        # This is late, and should be ignored.
        ("ALL", {"time": start_at + timedelta(seconds=1), "val": "h"}),
    ]

    flow.input("inp", TestingInput(inp))

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

    inp = [
        ("ALL", {"time": start_at, "val": "a"}),
        ("ALL", {"time": start_at + timedelta(seconds=4), "val": "b"}),
        ("ALL", {"time": start_at + timedelta(seconds=8), "val": "c"}),
        # The 10 second window should close just before processing this item.
        ("ALL", {"time": start_at + timedelta(seconds=12), "val": "d"}),
        ("ALL", {"time": start_at + timedelta(seconds=16), "val": "e"}),
    ]

    flow.input("inp", TestingInput(inp))

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
