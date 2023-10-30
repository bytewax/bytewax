from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_inspect():
    inp = ["a"]
    out = []
    seen = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.inspect("insp", lambda step_id, item: seen.append((step_id, item)))
    s.output("out", TestingSink(out))

    run_main(flow)

    # Check side-effects after execution is complete.
    assert seen == [("test_df.insp", "a")]


def test_inspect_debug():
    inp = ["a"]
    out = []
    seen = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.inspect_debug(
        "insp",
        lambda step_id, item, epoch, worker: seen.append(
            (step_id, item, epoch, worker)
        ),
    )
    s.output("out", TestingSink(out))

    run_main(flow)

    # Check side-effects after execution is complete.
    assert seen == [("test_df.insp", "a", 1, 0)]
