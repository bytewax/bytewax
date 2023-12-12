import re
from dataclasses import dataclass
from typing import Dict, List, Optional

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_operator_with_non_generic_streams():
    @operator
    def test_op(
        step_id: str,
        up: Stream,
    ) -> Stream:
        return up

    flow = Dataflow("test_df")
    inp = op.input("inp", flow, TestingSource([]))
    test_op("test_op", inp)


def test_operator_with_optional_argument():
    @operator
    def test_op(
        step_id: str,
        up: Stream[str],
        config: Optional[Dict[str, str]] = None,
    ) -> Stream[str]:
        return up

    flow = Dataflow("test_df")
    inp = op.input("inp", flow, TestingSource([]))
    test_op("test_op", inp)


def test_operator_with_named_downstreams():
    @dataclass
    class TestOut:
        a: Stream[int]
        b: Stream[int]

    @operator
    def test_op(
        step_id: str,
        up: Stream[int],
    ) -> TestOut:
        return TestOut(up, up)

    flow = Dataflow("test_df")
    inp = op.input("inp", flow, TestingSource([]))
    test_op("test_op", inp)


def test_operator_with_non_generic_downstreams():
    @dataclass
    class TestOut:
        a: Stream
        b: Stream

    @operator
    def test_op(
        step_id: str,
        up: Stream,
    ) -> TestOut:
        return TestOut(up, up)

    flow = Dataflow("test_df")
    inp = op.input("inp", flow, TestingSource([]))
    test_op("test_op", inp)


def test_raises_on_nested_stream():
    @operator
    def test_op(step_id: str, up: Stream, not_allowed: List[Stream]) -> Stream:
        return op.merge("merge", up, *not_allowed)

    flow = Dataflow("test_df")
    inp1 = op.input("inp1", flow, TestingSource([]))
    inp2 = op.input("inp2", flow, TestingSource([]))

    expect = "inconsistent stream scoping"
    with raises(AssertionError, match=re.escape(expect)):
        test_op("test_op", inp1, [inp2])


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
