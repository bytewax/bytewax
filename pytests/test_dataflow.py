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
    with raises(AssertionError, match=re.escape(expect)):
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


def test_step_id_check_str():
    flow = Dataflow("test_df")

    expect = "must be a `str`"
    with raises(TypeError, match=re.escape(expect)):
        op.input(1, flow, TestingSource([]))  # type: ignore


def test_step_id_check_periods():
    flow = Dataflow("test_df")

    expect = "can't contain any periods"
    with raises(ValueError, match=re.escape(expect)):
        op.input("1.5", flow, TestingSource([]))


def test_check_non_stream():
    expect = "must be a `Stream`"
    with raises(TypeError, match=re.escape(expect)):
        op.map("map", 1, lambda x: x)  # type: ignore


def test_check_non_stream_vararg():
    expect = "must be a `Stream`"
    with raises(TypeError, match=re.escape(expect)):
        op.merge("map", 1, 2, 3)  # type: ignore


def test_check_non_stream_varkwarg():
    expect = "must be a `Stream`"
    with raises(TypeError, match=re.escape(expect)):
        op.join_named("map", side_a=1, side_b=2)  # type: ignore
