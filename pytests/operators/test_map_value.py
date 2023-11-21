import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_map_value():
    inp = [0, 1, 2]
    out = []

    def add_one(item):
        return item + 1

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.map_value("add_one", s, add_one)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 1), ("ALL", 2), ("ALL", 3)]
