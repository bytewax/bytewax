from bytewax.dataflow import Dataflow
from bytewax.operators import _FoldFinalLogic
from bytewax.testing import TestingSink, TestingSource, run_main


def test_fold_final_logic_snapshot():
    def folder(old_state, value):
        return "new_state"

    logic = _FoldFinalLogic("test_step", folder, "old_state")

    logic.on_item(None, 5)

    assert logic.snapshot() == "new_state"


def test_fold_final():
    inp = [1, 4, 2, 9, 4, 3]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.fold_final("keep_max", lambda: 0, max)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("ALL", 9)]
