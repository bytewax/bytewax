import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_key_on():
    inp = [1, 2, 3]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda x: str(x))
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert out == [("1", 1), ("2", 2), ("3", 3)]


def test_key_on_raises_on_non_str_key():
    inp = [1, 2, 3]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda x: x)  # type: ignore
    op.output("out", s, TestingSink(out))

    expect = "must be a `str`"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
