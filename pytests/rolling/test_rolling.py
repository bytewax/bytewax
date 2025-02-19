from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock
from bytewax.rolling import rolling_delta_stateful_flat_map, rolling_flat_map
from bytewax.testing import TestingSink, TestingSource, run_main


def test_rolling_flat_map() -> None:
    flow = Dataflow("test_df")

    inp = [
        (datetime(2024, 9, 1, 2, 3, 4, tzinfo=timezone.utc), 1),
        (datetime(2024, 9, 1, 2, 3, 5, tzinfo=timezone.utc), 2),
        (datetime(2024, 9, 1, 2, 3, 6, tzinfo=timezone.utc), 3),
        (datetime(2024, 9, 1, 2, 3, 7, tzinfo=timezone.utc), 4),
        (datetime(2024, 9, 1, 2, 3, 8, tzinfo=timezone.utc), 5),
    ]
    inps = op.input("inp", flow, TestingSource(inp))
    keyed_inps = op.key_on("key", inps, lambda _: "ALL")

    def ts_getter(ts_val: Tuple[datetime, int]) -> datetime:
        ts, _val = ts_val
        return ts

    clock = EventClock(ts_getter, wait_for_system_duration=timedelta(seconds=0))

    def mapper(
        window: List[Tuple[datetime, int]],
    ) -> List[Tuple[Optional[int], Optional[int]]]:
        values = [val for ts, val in window]
        return [(min(values, default=None), max(values, default=None))]

    length = timedelta(seconds=3)
    rolling_out = rolling_flat_map(
        "min_max",
        keyed_inps,
        clock,
        length,
        mapper,
    )

    cleaned = op.key_rm("key_rm", rolling_out.downs)

    out: List[Tuple[Optional[int], Optional[int]]] = []
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)

    assert out == [
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 4),
        (3, 5),
        (None, None),
    ]


def test_rolling_delta_stateful_flat_map() -> None:
    flow = Dataflow("test_df")

    inp = [
        (datetime(2024, 9, 1, 2, 3, 4, tzinfo=timezone.utc), 1),
        (datetime(2024, 9, 1, 2, 3, 5, tzinfo=timezone.utc), 2),
        (datetime(2024, 9, 1, 2, 3, 6, tzinfo=timezone.utc), 3),
        (datetime(2024, 9, 1, 2, 3, 7, tzinfo=timezone.utc), 4),
        (datetime(2024, 9, 1, 2, 3, 8, tzinfo=timezone.utc), 5),
    ]
    inps = op.input("inp", flow, TestingSource(inp))
    keyed_inps = op.key_on("key", inps, lambda _: "ALL")

    def ts_getter(ts_val: Tuple[datetime, int]) -> datetime:
        ts, _val = ts_val
        return ts

    clock = EventClock(ts_getter, wait_for_system_duration=timedelta(seconds=0))

    def mapper(
        state: Optional[int],
        add: List[Tuple[datetime, int]],
        remove: List[Tuple[datetime, int]],
    ) -> Tuple[Optional[int], List[int]]:
        if state is None:
            state = 0

        for _ts, value in add:
            state += value
        for _ts, value in remove:
            state -= value

        return (state, [state])

    length = timedelta(seconds=3)
    rolling_out = rolling_delta_stateful_flat_map(
        "sum",
        keyed_inps,
        clock,
        length,
        mapper,
    )

    cleaned = op.key_rm("key_rm", rolling_out.downs)

    out: List[int] = []
    op.output("out", cleaned, TestingSink(out))

    run_main(flow)

    assert out == [
        1,
        3,
        6,
        9,
        12,
        0,
    ]
