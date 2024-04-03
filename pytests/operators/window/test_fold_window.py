from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict

import bytewax.operators as op
import bytewax.operators.window as win
from bytewax.dataflow import Dataflow
from bytewax.operators.window import (
    ZERO_TD,
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
)
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark

# TODO: Test snapshotting logic so we're sure a recovery roundtrip
# would work.


def _merge_defaultdict(
    a: Dict[str, int],
    b: Dict[str, int],
) -> Dict[str, int]:
    a.update(b)
    return a


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

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

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
    wo = win.fold_window(
        "count",
        s,
        clock,
        windower,
        lambda: defaultdict(int),
        count,
        _merge_defaultdict,
    )
    s = op.map_value("normal_dict", wo.down, map_dict)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, {"login": 1, "post": 2})),
        ("b", (1, {"login": 1})),
        ("a", (1, {"post": 1})),
        ("b", (2, {"post": 2})),
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

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = SessionWindower(gap=timedelta(seconds=5))

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    wo = win.fold_window("sum", s, clock, windower, list, add, list.__add__)
    op.output("out", wo.down, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (0, ["a", "b"])),
        ("a", (1, ["c", "d", "e", "f"])),
        ("a", (2, ["g"])),
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

    clock = EventClock(lambda e: e["time"], wait_for_system_duration=ZERO_TD)
    windower = SlidingWindower(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=align_to,
    )

    def add(acc, x):
        acc.append(x["val"])
        return acc

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    wo = win.fold_window("sum", s, clock, windower, list, add, list.__add__)
    op.output("out", wo.down, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (-1, ["a", "b"])),
        ("a", (0, ["a", "b", "c"])),
        ("a", (1, ["c", "d", "e", "f"])),
        ("a", (2, ["d", "e", "f", "g"])),
        ("a", (3, ["g"])),
    ]


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_fold_window_benchmark(benchmark, entry_point):
    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)

    inp = [align_to + timedelta(seconds=i) for i in range(100_000)]
    out = []

    flow = Dataflow("bench")
    times = op.input("in", flow, TestingSource(inp, 10))
    keyed_times = op.key_on("keyed", times, lambda _: "ALL")

    clock = EventClock(lambda x: x, wait_for_system_duration=ZERO_TD)
    windower = TumblingWindower(timedelta(minutes=1), align_to)

    wo = win.fold_window(
        "fold_window",
        keyed_times,
        clock,
        windower,
        lambda: None,
        lambda s, _: s,
        lambda s, _: s,
    )

    op.output("out", wo.down, TestingSink(out))

    expected = [("ALL", (i, None)) for i in range(1667)]

    def run():
        entry_point(flow)
        assert out == expected
        out.clear()

    benchmark(run)
