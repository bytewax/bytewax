from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_reduce_final():
    inp = [1, 4, 2, 9, 4, 3]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.reduce_final("keep_max", max)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 9)]
