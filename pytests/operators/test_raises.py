import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main
from pytest import raises


def test_raises():
    inp = [0, 1, 2]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.raises("raises", s)

    with raises(RuntimeError):
        run_main(flow)
