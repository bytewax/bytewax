from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter():
    inp = [1, 2, 3]
    out = []

    def is_odd(item):
        return item % 2 != 0

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.filter("is_odd", is_odd)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 3]
