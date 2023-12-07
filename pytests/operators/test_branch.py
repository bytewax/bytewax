import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_branch():
    inp = [1, 2, 3]
    out_odds = []
    out_evens = []

    def is_odd(x):
        return x % 2 != 0

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    branch_out = op.branch("branch", s, is_odd)
    odds = branch_out.trues
    evens = branch_out.falses
    op.output("out_odds", odds, TestingSink(out_odds))
    op.output("out_evens", evens, TestingSink(out_evens))

    run_main(flow)

    assert out_odds == [1, 3]
    assert out_evens == [2]


def test_branch_raises_on_non_bool_key():
    inp = [1, 2, 3]
    out_odds = []
    out_evens = []

    def not_a_predicate(x):
        return "not a bool"

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    branch_out = op.branch("branch", s, not_a_predicate)  # type: ignore
    odds = branch_out.trues
    evens = branch_out.falses
    op.output("out_odds", odds, TestingSink(out_odds))
    op.output("out_evens", evens, TestingSink(out_evens))

    expect = "must be a `bool`"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
