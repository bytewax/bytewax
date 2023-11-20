import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_reduce_final():
    inp = [1, 4, 2, 9, 4, 3]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.reduce_final("keep_max", s, max)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 9)]
