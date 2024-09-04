import re

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_stateful_map():
    inp = [2, 5, 8, 1, 3]
    out = []

    def running_mean(last_3, new):
        if last_3 is None:
            last_3 = []
        last_3.append(new)
        if len(last_3) > 3:
            last_3 = last_3[:-3]
        avg = sum(last_3) / len(last_3)
        return (last_3, avg)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful_map("running_mean", s, running_mean)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", 2.0),
        ("ALL", 3.5),
        ("ALL", 5.0),
        ("ALL", 2.0),
        ("ALL", 2.5),
    ]


def test_stateful_map_raises_on_non_tuple():
    inp = [1, 4, 2, 9, 4, 3]
    out = []

    def bad_mapper(state, val):
        return val

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful_map("bad_mapper", s, bad_mapper)
    op.output("out", s, TestingSink(out))

    expect = "must be a 2-tuple"
    with raises(RuntimeError):
        with raises(TypeError, match=re.escape(expect)):
            run_main(flow)
