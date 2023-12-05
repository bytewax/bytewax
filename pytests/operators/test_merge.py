import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_merge():
    inp_odds = [1, 3, 5]
    inp_evens = [2, 4, 6]
    inp_huge = [100, 200, 300]
    out = []

    flow = Dataflow("test_df")
    odds = op.input("inp_odds", flow, TestingSource(inp_odds))
    evens = op.input("inp_evens", flow, TestingSource(inp_evens))
    huge = op.input("inp_huge", flow, TestingSource(inp_huge))
    s = op.merge("merge", odds, evens, huge)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 100, 3, 4, 200, 5, 6, 300]
