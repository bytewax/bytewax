from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.ordering import order
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.windowing import EventClock


@dataclass(frozen=True)
class _Event:
    timestamp: datetime
    value: str


def test_order() -> None:
    flow = Dataflow("test_df")

    align_to = datetime(2024, 1, 1, tzinfo=timezone.utc)
    inp = [
        _Event(align_to, "a"),
        _Event(align_to + timedelta(seconds=2), "c"),
        _Event(align_to + timedelta(seconds=1), "b"),
        _Event(align_to + timedelta(seconds=14), "d"),
        _Event(align_to + timedelta(seconds=3), "e"),
    ]

    inps = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", inps, lambda _: "ALL")

    clock: EventClock[_Event] = EventClock(
        lambda e: e.timestamp, wait_for_system_duration=timedelta(seconds=10)
    )

    order_out = order("order", keyed, clock)

    def clean(key_event: Tuple[str, _Event]) -> str:
        _key, event = key_event
        return event.value

    cleaned = op.map("clean", order_out.down, clean)

    out: List[str] = []

    op.output("out", cleaned, TestingSink(out))

    run_main(flow)
    assert out == ["a", "b", "c", "d"]
