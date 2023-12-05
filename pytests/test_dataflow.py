import re
from typing import List

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


@operator
def bad_op_with_nested_stream(
    step_id: str, up: Stream, not_allowed: List[Stream]
) -> Stream:
    return op.merge("merge", up, *not_allowed)


def test_raises_on_nested_stream():
    flow = Dataflow("test_df")
    inp1 = op.input("inp1", flow, TestingSource([]))
    inp2 = op.input("inp2", flow, TestingSource([]))

    expect = "inconsistent stream scoping"
    with raises(ValueError, match=re.escape(expect)):
        bad_op_with_nested_stream("bad", inp1, [inp2])


def test_then():
    inp = [0, 1, 2]
    out = []

    def add_one(item):
        return item + 1

    flow = Dataflow("test_df")
    (
        op.input("inp", flow, TestingSource(inp))
        .then(op.map, "add_one", add_one)
        .then(op.output, "out", TestingSink(out))
    )

    run_main(flow)
    assert out == [1, 2, 3]


def test_multistream_raises_on_use_as_stream():
    inp = [0, 1, 2]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    ms = op.key_split("split", s, lambda _x: "KEY", lambda _x: "a", lambda _x: "b")

    expect = "`MultiStream` must be unpacked"
    with raises(TypeError, match=re.escape(expect)):
        op.map("map", ms, lambda s: s.upper())  # type: ignore
