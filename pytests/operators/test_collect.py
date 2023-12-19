import re
from typing import List, Union

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises
from typing_extensions import TypeGuard


def test_branch():
    inp = [1, 2, 3]
    out_odds = []
    out_evens = []

    def is_odd(x: int) -> bool:
        return x % 2 != 0

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    b_out = op.branch("branch", s, is_odd)
    odds = b_out.trues
    evens = b_out.falses
    op.output("out_odds", odds, TestingSink(out_odds))
    op.output("out_evens", evens, TestingSink(out_evens))

    run_main(flow)

    assert out_odds == [1, 3]
    assert out_evens == [2]


def test_branch_type():
    inp: List[Union[int, str]] = [1, "a", 2, "b"]
    out_ints = []
    out_strs = []

    def is_int(x: Union[int, str]) -> TypeGuard[int]:
        return isinstance(x, int)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    b_out = op.branch("branch", s, is_int)
    ints = b_out.trues
    strs = b_out.falses
    op.output("out_ints", ints, TestingSink(out_ints))
    op.output("out_strs", strs, TestingSink(out_strs))

    run_main(flow)

    assert out_ints == [1, 2]
    assert out_strs == ["a", "b"]


def test_branch_raises_on_non_bool_key():
    inp = [1, 2, 3]
    out_odds = []
    out_evens = []

    def not_a_predicate(x):
        return "not a bool"

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    b_out = op.branch("branch", s, not_a_predicate)  # type: ignore
    odds = b_out.trues
    evens = b_out.falses
    op.output("out_odds", odds, TestingSink(out_odds))
    op.output("out_evens", evens, TestingSink(out_evens))

    expect = "must be a `bool`"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
