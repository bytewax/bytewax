from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_map():
    inp = [0, 1, 2]
    out = []

    def add_one(item):
        return item + 1

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.map("add_one", add_one)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]
