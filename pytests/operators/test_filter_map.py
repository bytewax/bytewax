from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_filter_map():
    inp = [0, 1, 2, 3, 4, 5]
    out = []

    def make_odd(item):
        if item % 2 != 0:
            return None
        return item + 1

    flow = Dataflow("test_df")
    flow.input("inp", TestingSource(inp)).filter_map("make_odd", make_odd).output(
        "out", TestingSink(out)
    )

    run_main(flow)
    assert out == [1, 3, 5]
