"""Late items on every windowing operator.

Whether an item is late, and which windows it is reported late for,
depends only on the clock and the windower, so every operator must
emit the same `late` stream for the same input.

"""

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow, Stream
from bytewax.operators.windowing import (
    LATE_SESSION_ID,
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
    Windower,
    WindowOut,
)
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark

ALIGN_TO = datetime(2022, 1, 1, tzinfo=timezone.utc)

# Seconds since `ALIGN_TO`. With a 5 second wait the watermark is the
# largest timestamp so far minus 5, so 8 is on time (watermark 7) but
# 3 is late (watermark 7), 14 is late (watermark 20), and 24 is late
# (watermark 25).
INP = [0, 12, 8, 3, 25, 14, 21, 30, 24]
LATE = {3, 14, 24}

# Window IDs each late item would have been in.
EXPECTED_LATE: Dict[str, List[Tuple[int, int]]] = {
    # 10 second windows: ID is the timestamp // 10.
    "tumbling": [(0, 3), (1, 14), (2, 24)],
    # 10 second windows every 5 seconds: window ID `i` covers
    # `[5 * i, 5 * i + 10)`, so each item is in two windows.
    "sliding": [(-1, 3), (0, 3), (1, 14), (2, 14), (3, 24), (4, 24)],
    # Sessions can't know which session a late item belonged to.
    "session": [(LATE_SESSION_ID, 3), (LATE_SESSION_ID, 14), (LATE_SESSION_ID, 24)],
}

WINDOWERS: Dict[str, Callable[[], Windower[Any]]] = {
    "tumbling": lambda: TumblingWindower(
        length=timedelta(seconds=10), align_to=ALIGN_TO
    ),
    "sliding": lambda: SlidingWindower(
        length=timedelta(seconds=10), offset=timedelta(seconds=5), align_to=ALIGN_TO
    ),
    "session": lambda: SessionWindower(gap=timedelta(seconds=3)),
}


def _clock() -> EventClock[int]:
    # Freeze system time so the watermark only depends on the items.
    return EventClock(
        lambda secs: ALIGN_TO + timedelta(seconds=secs),
        wait_for_system_duration=timedelta(seconds=5),
        now_getter=lambda: ALIGN_TO,
        to_system_utc=lambda _: None,
    )


def _drop_side(id_side_value: Tuple[int, Tuple[int, int]]) -> Tuple[int, int]:
    window_id, (_side, value) = id_side_value
    return (window_id, value)


def _join(s: Stream[Tuple[str, int]], windower: Windower[Any]) -> WindowOut:
    b = op.branch("side", s, lambda kv: kv[1] % 2 == 0)
    wo = win.join_window("w", _clock(), windower, b.trues, b.falses)
    late = op.map_value("drop_side", wo.late, _drop_side)
    return WindowOut(wo.down, late, wo.meta)


OPS: Dict[str, Callable[[Stream[Tuple[str, int]], Windower[Any]], WindowOut]] = {
    "collect": lambda s, w: win.collect_window("w", s, _clock(), w),
    "collect_unordered": lambda s, w: win.collect_window(
        "w", s, _clock(), w, ordered=False
    ),
    "fold": lambda s, w: win.fold_window(
        "w", s, _clock(), w, list, lambda acc, x: acc + [x], list.__add__
    ),
    "reduce": lambda s, w: win.reduce_window("w", s, _clock(), w, max),
    "max": lambda s, w: win.max_window("w", s, _clock(), w),
    "min": lambda s, w: win.min_window("w", s, _clock(), w),
    "count": lambda s, w: win.count_window(
        "w", op.key_rm("unkey", s), _clock(), w, lambda _: "ALL"
    ),
    "join": _join,
}


@mark.parametrize("windower_name", list(WINDOWERS))
@mark.parametrize("op_name", list(OPS))
def test_late_items(op_name: str, windower_name: str) -> None:
    late: List[Tuple[str, Tuple[int, int]]] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP))
    keyed = op.key_on("key", s, lambda _: "ALL")
    wo = OPS[op_name](keyed, WINDOWERS[windower_name]())
    op.output("late", wo.late, TestingSink(late))

    run_main(flow)
    assert late == [("ALL", id_v) for id_v in EXPECTED_LATE[windower_name]]


@mark.parametrize("windower_name", list(WINDOWERS))
def test_late_items_not_in_windows(windower_name: str) -> None:
    down: List[Tuple[str, Tuple[int, List[int]]]] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP))
    keyed = op.key_on("key", s, lambda _: "ALL")
    wo = win.collect_window("w", keyed, _clock(), WINDOWERS[windower_name]())
    op.output("down", wo.down, TestingSink(down))

    run_main(flow)
    collected = {x for _, (_, xs) in down for x in xs}
    assert collected == set(INP) - LATE


def test_late_item_left_out_of_still_open_sliding_window() -> None:
    # 17 arrives when the watermark is 20, so it is late. Its windows
    # are 2 `[10, 20)`, already closed, and 3 `[15, 25)`, still open.
    # It is reported late for both and left out of both.
    inp = [0, 25, 17, 21]
    late: List[Tuple[str, Tuple[int, int]]] = []
    down: List[Tuple[str, Tuple[int, List[int]]]] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", s, lambda _: "ALL")
    wo = win.collect_window("w", keyed, _clock(), WINDOWERS["sliding"]())
    op.output("late", wo.late, TestingSink(late))
    op.output("down", wo.down, TestingSink(down))

    run_main(flow)
    assert late == [("ALL", (2, 17)), ("ALL", (3, 17))]
    assert ("ALL", (3, [21])) in down
