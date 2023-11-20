import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_inspect():
    inp = ["a"]
    out = []
    seen = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.inspect("insp", s, lambda step_id, item: seen.append((step_id, item)))
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check side-effects after execution is complete.
    assert seen == [("test_df.insp", "a")]


def test_inspect_debug():
    inp = ["a"]
    out = []
    seen = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.inspect_debug(
        "insp",
        s,
        lambda step_id, item, epoch, worker: seen.append(
            (step_id, item, epoch, worker)
        ),
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)

    # Check side-effects after execution is complete.
    assert seen == [("test_df.insp", "a", 1, 0)]
