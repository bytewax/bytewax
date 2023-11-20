import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_max_final():
    inp = [
        {"user": "a", "val": 1},
        {"user": "a", "val": 9},
        {"user": "a", "val": 3},
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    s = op.max_final("max", s, by=lambda e: e["val"])
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("a", {"user": "a", "val": 9})]


def test_min_final():
    inp = [
        {"user": "a", "val": 1},
        {"user": "a", "val": 9},
        {"user": "a", "val": 3},
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key_on_user", s, lambda e: e["user"])
    s = op.min_final("min", s, by=lambda e: e["val"])
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("a", {"user": "a", "val": 1})]
