import re
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic, _StatefulFlatMapLogic
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


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


def test_stateful_flat_map_discard_mid_batch_starts_over():
    inp = [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("a", 4)]
    out = []

    def mapper(state, value):
        # Discard on 2. The next value starts again from no state.
        new_state = None if value == 2 else (state or 0) + value
        return new_state, [(state, value)]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp, batch_size=5))
    s = op.stateful_flat_map("discard", s, mapper)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("a", (None, 1)),
        ("a", (1, 2)),
        ("a", (None, 3)),
        ("a", (3, 4)),
        ("b", (None, 1)),
    ]


def test_stateful_flat_map_raises_on_non_tuple():
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([("a", 1)]))
    s = op.stateful_flat_map("bad_mapper", s, lambda _state, value: value)
    op.output("out", s, TestingSink([]))

    expect = "must be a 2-tuple"
    with raises(RuntimeError):
        with raises(TypeError, match=re.escape(expect)):
            run_main(flow)


def test_stateful_flat_map_resumes_snapshot(recovery_config):
    inp = [("a", 1), ("b", 5), TestingSource.ABORT(), ("a", 2), ("b", 6)]
    out = []

    def mapper(state, value):
        total = (state or 0) + value
        return total, [total]

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_flat_map("sum", s, mapper)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)
    assert out == [("a", 1), ("b", 5)]

    out.clear()
    run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)
    assert out == [("a", 3), ("b", 11)]
