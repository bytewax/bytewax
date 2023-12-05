import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators.helpers import map_dict_value
from bytewax.testing import TestingSink, TestingSource, run_main


def test_map_dict_value():
    inp = [
        {"a": 0, "b": 0},
        {"a": 1, "b": 0},
        {"a": 2, "b": 0},
    ]
    out = []

    def add_one(item):
        return item + 1

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.map("add_one", s, map_dict_value("a", add_one))
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        {"a": 1, "b": 0},
        {"a": 2, "b": 0},
        {"a": 3, "b": 0},
    ]
