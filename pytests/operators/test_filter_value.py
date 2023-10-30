from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter_value():
    inp = [1, 2, 3]
    out = []

    def is_odd(item):
        return item % 2 != 0

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.filter_value("is_odd", is_odd)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 1), ("ALL", 3)]
