import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_count_final():
    inp = ["a", "a", "b", "c", "b", "a"]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.count_final("count", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("a", 3), ("b", 2), ("c", 1)]
