import re

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_key_on():
    inp = [1, 2, 3]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda x: str(x))
    s.output("out", TestingSink(out))

    run_main(flow)

    assert out == [("1", 1), ("2", 2), ("3", 3)]


def test_key_on_raises_on_non_str_key():
    inp = [1, 2, 3]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda x: x)
    s.output("out", TestingSink(out))

    expect = "must be a `str`"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
