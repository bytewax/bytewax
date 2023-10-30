from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_batch():
    inp = list(range(10))
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    # Use a long timeout to avoid triggering that.
    # We can't easily test system time based behavior.
    s = s.batch("batch", timedelta(seconds=10), 3)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", [0, 1, 2]),
        ("ALL", [3, 4, 5]),
        ("ALL", [6, 7, 8]),
        ("ALL", [9]),
    ]
