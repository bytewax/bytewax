from collections import defaultdict
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.dataflow import Dataflow
from bytewax.operators.window import (
    EventClockConfig,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowMetadata,
)
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)

# TODO: Test snapshotting logic so we're sure a recovery roundtrip
# would work.


# TODO: When we have a "window unary" operator, move tests of the
# actual windowing semantics to that. For now, since `fold_window` is
# the base of the window operators, test here.


def test_fold_window_tumbling():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        {"time": align_to, "user": "a", "type": "login"},
        {"time": align_to + timedelta(seconds=4), "user": "a", "type": "post"},
        {"time": align_to + timedelta(seconds=8), "user": "a", "type": "post"},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "b", "type": "login"},
        {"time": align_to + timedelta(seconds=16), "user": "a", "type": "post"},
        # Second 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=20), "user": "b", "type": "post"},
        {"time": align_to + timedelta(seconds=24), "user": "b", "type": "post"},
    ]
    out = []

    clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def count(counts, event):
        typ = event["type"]
        counts[typ] += 1
        return counts

    def map_dict(metadata_value):
        metadata, value = metadata_value
        return (metadata, dict(value))

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    s = win.fold_window("count", s, clock, windower, lambda: defaultdict(int), count)
    s = op.map_value("normal_dict", s, map_dict)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        (
            "a",
            (
                WindowMetadata(align_to, align_to + timedelta(seconds=10)),
                {"login": 1, "post": 2},
            ),
        ),
        (
            "b",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=10), align_to + timedelta(seconds=20)
                ),
                {"login": 1},
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=10), align_to + timedelta(seconds=20)
                ),
                {"post": 1},
            ),
        ),
        (
            "b",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=20), align_to + timedelta(seconds=30)
                ),
                {"post": 2},
            ),
        ),
    ]


def test_fold_window_session():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    inp = [
        # Session 1
        {"time": align_to + timedelta(seconds=1), "user": "a", "val": "a"},
        {"time": align_to + timedelta(seconds=5), "user": "a", "val": "b"},
        # Session 2
        {"time": align_to + timedelta(seconds=11), "user": "a", "val": "c"},
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": "d"},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": "e"},
        {"time": align_to + timedelta(seconds=14), "user": "a", "val": "f"},
        # Session 3
        {"time": align_to + timedelta(seconds=20), "user": "a", "val": "g"},
        # This is late, and should be ignored
        {"time": align_to + timedelta(seconds=1), "user": "a", "val": "h"},
    ]
    out = []

    clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = SessionWindow(gap=timedelta(seconds=5))

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    s = win.fold_window("sum", s, clock, windower, list, add)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=1), align_to + timedelta(seconds=5)
                ),
                ["a", "b"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=11), align_to + timedelta(seconds=14)
                ),
                ["c", "d", "e", "f"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=20), align_to + timedelta(seconds=20)
                ),
                ["g"],
            ),
        ),
    ]


def test_fold_window_sliding():
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    # Valign_to
    #  a  b   c   def g
    #  h
    # -----)
    # [---------)
    #      [---------)
    #           [---------)
    #                [---------)
    inp = [
        {"time": align_to + timedelta(seconds=1), "user": "a", "val": "a"},
        {"time": align_to + timedelta(seconds=4), "user": "a", "val": "b"},
        {"time": align_to + timedelta(seconds=8), "user": "a", "val": "c"},
        {"time": align_to + timedelta(seconds=12), "user": "a", "val": "d"},
        {"time": align_to + timedelta(seconds=13), "user": "a", "val": "e"},
        {"time": align_to + timedelta(seconds=14), "user": "a", "val": "f"},
        {"time": align_to + timedelta(seconds=16), "user": "a", "val": "g"},
        # This is late, and should be ignored.
        {"time": align_to + timedelta(seconds=1), "user": "a", "val": "h"},
    ]
    out = []

    clock = EventClockConfig(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = SlidingWindow(
        length=timedelta(seconds=10), align_to=align_to, offset=timedelta(seconds=5)
    )

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    s = win.fold_window("sum", s, clock, windower, list, add)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        (
            "a",
            (
                WindowMetadata(
                    align_to - timedelta(seconds=5), align_to + timedelta(seconds=5)
                ),
                ["a", "b"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(align_to, align_to + timedelta(seconds=10)),
                ["a", "b", "c"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=5), align_to + timedelta(seconds=15)
                ),
                ["c", "d", "e", "f"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=10), align_to + timedelta(seconds=20)
                ),
                ["d", "e", "f", "g"],
            ),
        ),
        (
            "a",
            (
                WindowMetadata(
                    align_to + timedelta(seconds=15), align_to + timedelta(seconds=25)
                ),
                ["g"],
            ),
        ),
    ]


def build_fold_window_dataflow(out) -> Dataflow:
    clock_config = win.EventClockConfig(
        dt_getter=lambda x: x,
        wait_for_system_duration=timedelta(seconds=0),
    )
    window = win.TumblingWindow(
        align_to=datetime(2022, 1, 1, tzinfo=timezone.utc), length=timedelta(minutes=1)
    )

    records = [
        datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i)
        for i in range(100_000)
    ]
    flow = Dataflow("bench")
    (
        op.input("in", flow, TestingSource(records, 10))
        .then(op.key_on, "key-on", lambda _: "x")
        .then(
            win.fold_window,
            "fold-window",
            clock_config,
            window,
            lambda: None,
            lambda s, _: s,
        )
        .then(op.output, "stdout", TestingSink(out))
    )

    return flow


def run_fold_window_dataflow(entry_point, flow, out, expected):
    entry_point(flow)
    assert out == expected
    out.clear()


def test_fold_window_benchmark(benchmark, entry_point):
    out = []
    expected = [
        (
            "x",
            (
                WindowMetadata(
                    open_time=datetime(2024, 1, 1, tzinfo=timezone.utc)
                    + timedelta(minutes=i),
                    close_time=datetime(2024, 1, 1, tzinfo=timezone.utc)
                    + timedelta(minutes=i + 1),
                ),
                None,
            ),
        )
        for i in range(1667)
    ]
    flow = build_fold_window_dataflow(out)
    benchmark(lambda: run_fold_window_dataflow(entry_point, flow, out, expected))
