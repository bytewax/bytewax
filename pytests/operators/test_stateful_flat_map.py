import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic, _StatefulFlatMapLogic
from bytewax.testing import TestingSink, TestingSource, run_main


def test_stateful_map_logic_discard_on_none():
    def mapper(old_state, value):
        assert old_state is None
        return None, None

    logic = _StatefulFlatMapLogic("test_step", mapper, None)
    (out, discard) = logic.on_item(1)

    assert discard == StatefulLogic.DISCARD


def test_stateful_map_logic_snapshot():
    def mapper(old_state, value):
        assert old_state is None
        return "new_state", None

    logic = _StatefulFlatMapLogic("test_step", mapper, None)
    logic.on_item(1)

    assert logic.snapshot() == "new_state"


def test_stateful_flat_map():
    inp = [2, 5, 8, 1, 3]
    out = []

    def filter_smaller(last, new):
        if last is None:
            return (new, [new])
        elif new > last:
            return (new, [new])
        else:
            return (new, [])

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful_flat_map("filter_smaller", s, filter_smaller)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", 2),
        ("ALL", 5),
        ("ALL", 8),
        ("ALL", 3),
    ]
