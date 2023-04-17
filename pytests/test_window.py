from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingInput, TestingOutput
from bytewax.window import (
    EventClockConfig,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
)


def test_session_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    inp = [
        # Session 1
        ("ALL", {"time": align_to + timedelta(seconds=1), "val": "a"}),
        ("ALL", {"time": align_to + timedelta(seconds=5), "val": "b"}),
        # Session 2
        ("ALL", {"time": align_to + timedelta(seconds=11), "val": "c"}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": "d"}),
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": "e"}),
        ("ALL", {"time": align_to + timedelta(seconds=14), "val": "f"}),
        # Session 3
        ("ALL", {"time": align_to + timedelta(seconds=20), "val": "g"}),
        # This is late, and should be ignored
        ("ALL", {"time": align_to + timedelta(seconds=1), "val": "h"}),
    ]

    flow.input("inp", TestingInput(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = SessionWindow(gap=timedelta(seconds=5))

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", ["a", "b"]),
            ("ALL", ["c", "d", "e", "f"]),
            ("ALL", ["g"]),
        ]
    )


def test_sliding_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    # Valign_to
    #  a  b   c   def g
    #  h
    # -----)
    # [---------)
    #      [---------)
    #           [---------)
    #                [---------)
    inp = [
        ("ALL", {"time": align_to + timedelta(seconds=1), "val": "a"}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": "b"}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": "c"}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": "d"}),
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": "e"}),
        ("ALL", {"time": align_to + timedelta(seconds=14), "val": "f"}),
        ("ALL", {"time": align_to + timedelta(seconds=16), "val": "g"}),
        # This is late, and should be ignored.
        ("ALL", {"time": align_to + timedelta(seconds=1), "val": "h"}),
    ]

    flow.input("inp", TestingInput(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = SlidingWindow(
        length=timedelta(seconds=10), align_to=align_to, offset=timedelta(seconds=5)
    )

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", ["a", "b"]),
            ("ALL", ["a", "b", "c"]),
            ("ALL", ["c", "d", "e", "f"]),
            ("ALL", ["d", "e", "f", "g"]),
            ("ALL", ["g"]),
        ]
    )


def test_tumbling_window():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)

    flow = Dataflow()

    inp = [
        ("ALL", {"time": align_to, "val": "a"}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": "b"}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": "c"}),
        # The 10 second window should close just before processing this item.
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": "d"}),
        ("ALL", {"time": align_to + timedelta(seconds=16), "val": "e"}),
    ]

    flow.input("inp", TestingInput(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted([("ALL", ["a", "b", "c"]), ("ALL", ["d", "e"])])
