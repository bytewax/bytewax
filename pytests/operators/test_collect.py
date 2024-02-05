from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import _CollectLogic, _CollectState
from bytewax.testing import TestingSink, TestingSource, run_main


def test_collect_logic_snapshot():
    now = datetime(2023, 1, 1, tzinfo=timezone.utc)
    timeout = timedelta(seconds=10)
    logic = _CollectLogic("test_step", lambda: now, timeout, 3, _CollectState())

    logic.on_item(1)

    assert logic.snapshot() == _CollectState([1], now + timeout)


def test_collect():
    inp = list(range(10))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    # Use a long timeout to avoid triggering that.
    # We can't easily test system time based behavior.
    s = op.collect("collect", s, timedelta(seconds=10), 3)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", [0, 1, 2]),
        ("ALL", [3, 4, 5]),
        ("ALL", [6, 7, 8]),
        ("ALL", [9]),
    ]
