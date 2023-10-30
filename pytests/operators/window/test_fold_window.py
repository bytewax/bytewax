from collections import defaultdict
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.window import (
    EventClockConfig,
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
)

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

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.fold_window("count", clock, windower, lambda: defaultdict(int), count)
    s = s.map_value("normal_dict", dict)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", {"login": 1, "post": 2}),
        ("b", {"login": 1}),
        ("a", {"post": 1}),
        ("b", {"post": 2}),
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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.fold_window("sum", clock, windower, list, add)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", ["a", "b"]),
        ("a", ["c", "d", "e", "f"]),
        ("a", ["g"]),
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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.fold_window("sum", clock, windower, list, add)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", ["a", "b"]),
        ("a", ["a", "b", "c"]),
        ("a", ["c", "d", "e", "f"]),
        ("a", ["d", "e", "f", "g"]),
        ("a", ["g"]),
    ]
