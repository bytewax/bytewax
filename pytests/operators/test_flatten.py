import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_flatten():
    inp = [[1, 2], [], [3]]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flatten("flatten", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]


def test_flatten_raises():
    inp = [666]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flatten("flatten", s)  # type: ignore
    op.output("out", s, TestingSink(out))

    expect = "to be iterables"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
