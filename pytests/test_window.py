import pickle
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.window import (
    EventClockConfig,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowMetadata,
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

    flow.input("inp", TestingSource(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = SessionWindow(gap=timedelta(seconds=5))

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.output("out", TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=1),
                        align_to + timedelta(seconds=5),
                    ),
                    ["a", "b"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=11),
                        align_to + timedelta(seconds=14),
                    ),
                    ["c", "d", "e", "f"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=20),
                        align_to + timedelta(seconds=20),
                    ),
                    ["g"],
                ),
            ),
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

    flow.input("inp", TestingSource(inp))

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
    flow.output("out", TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to - timedelta(seconds=5),
                        align_to + timedelta(seconds=5),
                    ),
                    ["a", "b"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to,
                        align_to + timedelta(seconds=10),
                    ),
                    ["a", "b", "c"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=5),
                        align_to + timedelta(seconds=15),
                    ),
                    ["c", "d", "e", "f"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=10),
                        align_to + timedelta(seconds=20),
                    ),
                    ["d", "e", "f", "g"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=15),
                        align_to + timedelta(seconds=25),
                    ),
                    ["g"],
                ),
            ),
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

    flow.input("inp", TestingSource(inp))

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow.fold_window("sum", clock_config, window_config, list, add)

    out = []
    flow.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(
        [
            (
                "ALL",
                (
                    WindowMetadata(align_to, align_to + timedelta(seconds=10)),
                    ["a", "b", "c"],
                ),
            ),
            (
                "ALL",
                (
                    WindowMetadata(
                        align_to + timedelta(seconds=10),
                        align_to + timedelta(seconds=20),
                    ),
                    ["d", "e"],
                ),
            ),
        ]
    )


def test_windowmetadata_pickles():
    meta = WindowMetadata(datetime.now(timezone.utc), datetime.now(timezone.utc))

    dumped = pickle.dumps(meta)
    assert pickle.loads(dumped) == meta
