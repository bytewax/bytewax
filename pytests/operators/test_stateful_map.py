import re

from bytewax.dataflow import Dataflow
from bytewax.operators import UnaryLogic, _StatefulMapLogic
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_stateful_map_logic_discard_on_none():
    def mapper(old_state, value):
        assert old_state is None
        return None, None

    logic = _StatefulMapLogic("test_step", mapper, None)
    (out, discard) = logic.on_item(None, 1)

    assert discard == UnaryLogic.DISCARD


def test_stateful_map_logic_snapshot():
    def mapper(old_state, value):
        assert old_state is None
        return "new_state", None

    logic = _StatefulMapLogic("test_step", mapper, None)
    logic.on_item(None, 1)

    assert logic.snapshot() == "new_state"


def test_stateful_map():
    inp = [2, 5, 8, 1, 3]
    out = []

    def running_mean(last_3, new):
        last_3.append(new)
        if len(last_3) > 3:
            last_3 = last_3[:-3]
        avg = sum(last_3) / len(last_3)
        return (last_3, avg)

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.stateful_map("running_mean", list, running_mean)
    s.output("out", TestingSink(out))

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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.stateful_map("bad_mapper", lambda: None, bad_mapper)
    s.output("out", TestingSink(out))

    expect = "must be a 2-tuple"
    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)
