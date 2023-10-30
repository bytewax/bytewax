from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
from pytest import raises


def test_flow_requires_output():
    inp = range(3)

    flow = Dataflow("test_df")
    flow.input("inp", TestingSource(inp))

    with raises(ValueError):
        run_main(flow)
