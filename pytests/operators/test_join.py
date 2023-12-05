import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import _JoinLogic, _JoinState
from bytewax.testing import TestingSink, TestingSource, run_main


def test_join_logic_snapshot(now):
    logic = _JoinLogic("test_step", False, _JoinState.for_names(["a", "b", "c"]))

    logic.on_item(now, ("a", 1))
    logic.on_item(now, ("b", 2))

    expect = _JoinState({"a": [1], "b": [2], "c": []})
    assert logic.snapshot() == expect


def test_join_logic_astuples():
    state = _JoinState.for_names(["a", "b", "c"])
    state.add_val("a", 1)
    state.add_val("a", 2)
    state.add_val("c", 3)
    state.add_val("c", 4)

    assert list(state.astuples()) == [
        (1, None, 3),
        (1, None, 4),
        (2, None, 3),
        (2, None, 4),
    ]


def test_join_logic_asdicts():
    state = _JoinState.for_names(["a", "b", "c"])
    state.add_val("a", 1)
    state.add_val("a", 2)
    state.add_val("c", 3)
    state.add_val("c", 4)

    assert list(state.asdicts()) == [
        {"a": 1, "c": 3},
        {"a": 1, "c": 4},
        {"a": 2, "c": 3},
        {"a": 2, "c": 4},
    ]


def test_join():
    inp_l = [1]
    inp_r = [2]
    out = []

    flow = Dataflow("test_df")
    left = op.input("inp_l", flow, TestingSource(inp_l))
    left = op.key_on("key_l", left, lambda _x: "ALL")
    right = op.input("inp_r", flow, TestingSource(inp_r))
    right = op.key_on("key_r", right, lambda _x: "ALL")
    s = op.join("join", left, right)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", (1, 2))]


def test_join_running():
    inp_l = [1]
    inp_r = [2, 3]
    out = []

    flow = Dataflow("test_df")
    left = op.input("inp_l", flow, TestingSource(inp_l))
    left = op.key_on("key_l", left, lambda _x: "ALL")
    right = op.input("inp_r", flow, TestingSource(inp_r))
    right = op.key_on("key_r", right, lambda _x: "ALL")
    s = op.join("join", left, right, running=True)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", (1, None)), ("ALL", (1, 2)), ("ALL", (1, 3))]


def test_join_named():
    inp_l = [1]
    inp_r = [2]
    out = []

    flow = Dataflow("test_df")
    left = op.input("inp_l", flow, TestingSource(inp_l))
    left = op.key_on("key_l", left, lambda _x: "ALL")
    right = op.input("inp_r", flow, TestingSource(inp_r))
    right = op.key_on("key_r", right, lambda _x: "ALL")
    s = op.join_named("join", left=left, right=right)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("ALL", {"left": 1, "right": 2})]


def test_join_named_running():
    inp_l = [1]
    inp_r = [2, 3]
    out = []

    flow = Dataflow("test_df")
    left = op.input("inp_l", flow, TestingSource(inp_l))
    left = op.key_on("key_l", left, lambda _x: "ALL")
    right = op.input("inp_r", flow, TestingSource(inp_r))
    right = op.key_on("key_r", right, lambda _x: "ALL")
    s = op.join_named("join", running=True, left=left, right=right)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", {"left": 1}),
        ("ALL", {"left": 1, "right": 2}),
        ("ALL", {"left": 1, "right": 3}),
    ]
