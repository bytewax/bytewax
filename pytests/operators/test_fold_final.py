import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import _FoldFinalLogic
from bytewax.testing import TestingSink, TestingSource, run_main


def test_fold_final_logic_snapshot(now):
    def folder(old_state, value):
        return "new_state"

    logic = _FoldFinalLogic("test_step", folder, "old_state")

    logic.on_item(now, 5)

    assert logic.snapshot() == "new_state"


def test_fold_final():
    inp = [1, 4, 2, 9, 4, 3]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.fold_final("keep_max", s, lambda: 0, max)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 9)]
