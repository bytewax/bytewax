from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_count_final():
    inp = ["a", "a", "b", "c", "b", "a"]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.count_final("count", lambda x: x)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("a", 3), ("b", 2), ("c", 1)]
