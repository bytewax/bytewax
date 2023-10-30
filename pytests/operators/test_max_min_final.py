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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.max_final("max", by=lambda e: e["val"])
    s.output("out", TestingSink(out))

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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key_on_user", lambda e: e["user"])
    s = s.min_final("min", by=lambda e: e["val"])
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("a", {"user": "a", "val": 1})]
