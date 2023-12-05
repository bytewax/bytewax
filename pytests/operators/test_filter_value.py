import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter_value():
    inp = [1, 2, 3]
    out = []

    def is_odd(item):
        return item % 2 != 0

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.filter_value("is_odd", s, is_odd)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 1), ("ALL", 3)]
