from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_merge():
    inp_odds = [1, 3, 5]
    inp_evens = [2, 4, 6]
    inp_huge = [100, 200, 300]
    out = []

    flow = Dataflow("test_df")
    odds = flow.input("inp_odds", TestingSource(inp_odds))
    evens = flow.input("inp_evens", TestingSource(inp_evens))
    huge = flow.input("inp_huge", TestingSource(inp_huge))
    s = odds.merge("merge", evens, huge)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 100, 3, 4, 200, 5, 6, 300]


def test_merge_all():
    inp_odds = [1, 3, 5]
    inp_evens = [2, 4, 6]
    inp_huge = [100, 200, 300]
    out = []

    flow = Dataflow("test_df")
    odds = flow.input("inp_odds", TestingSource(inp_odds))
    evens = flow.input("inp_evens", TestingSource(inp_evens))
    huge = flow.input("inp_huge", TestingSource(inp_huge))
    s = flow.merge_all("merge", odds, evens, huge)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 100, 3, 4, 200, 5, 6, 300]
