import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter_map_value():
    inp = [
        ("ALL", 0),
        ("ALL", 1),
        ("ALL", 2),
        ("ALL", 3),
        ("ALL", 4),
        ("ALL", 5),
    ]
    out = []

    def make_odd(item):
        if item % 2 != 0:
            return None
        return item + 1

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.filter_map_value("make_odd", s, make_odd)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", 1),
        ("ALL", 3),
        ("ALL", 5),
    ]
