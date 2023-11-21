import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter():
    inp = [1, 2, 3]
    out = []

    def is_odd(item):
        return item % 2 != 0

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.filter("is_odd", s, is_odd)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 3]
