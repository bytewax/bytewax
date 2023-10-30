import re

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_flatten():
    inp = [[1, 2], [], [3]]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.flatten("flatten")
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [1, 2, 3]


def test_flatten_raises():
    inp = [666]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.flatten("flatten")
    s.output("out", TestingSink(out))

    expect = "to be iterables"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
