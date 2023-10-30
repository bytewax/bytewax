from bytewax.dataflow import Dataflow
from bytewax.operators import _JoinState
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.window import EventClockConfig, TumblingWindow


def test_join_logic_astuples():
    state = _JoinState(["a", "b", "c"])
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
    state = _JoinState(["a", "b", "c"])
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
    left = flow.input("inp_l", TestingSource(inp_l)).key_on("key_l", lambda _x: "ALL")
    right = flow.input("inp_r", TestingSource(inp_r)).key_on("key_r", lambda _x: "ALL")
    left.join("join", right).output("out", TestingSink(out))

    run_main(flow)

    assert out == [("ALL", (1, 2))]


def test_join_running():
    inp_l = [1]
    inp_r = [2, 3]
    out = []

    flow = Dataflow("test_df")
    left = flow.input("inp_l", TestingSource(inp_l)).key_on("key_l", lambda _x: "ALL")
    right = flow.input("inp_r", TestingSource(inp_r)).key_on("key_r", lambda _x: "ALL")
    left.join("join", right, running=True).output("out", TestingSink(out))

    run_main(flow)

    assert out == [("ALL", (1, None)), ("ALL", (1, 2)), ("ALL", (1, 3))]


def test_join_named():
    inp_l = [1]
    inp_r = [2]
    out = []

    flow = Dataflow("test_df")
    left = flow.input("inp_l", TestingSource(inp_l)).key_on("key_l", lambda _x: "ALL")
    right = flow.input("inp_r", TestingSource(inp_r)).key_on("key_r", lambda _x: "ALL")
    flow.join_named("join", left=left, right=right).output("out", TestingSink(out))

    run_main(flow)

    assert out == [("ALL", {"left": 1, "right": 2})]


def test_join_named_running():
    inp_l = [1]
    inp_r = [2, 3]
    out = []

    flow = Dataflow("test_df")
    left = flow.input("inp_l", TestingSource(inp_l)).key_on("key_l", lambda _x: "ALL")
    right = flow.input("inp_r", TestingSource(inp_r)).key_on("key_r", lambda _x: "ALL")
    flow.join_named("join", running=True, left=left, right=right).output(
        "out", TestingSink(out)
    )

    run_main(flow)

    assert out == [
        ("ALL", {"left": 1}),
        ("ALL", {"left": 1, "right": 2}),
        ("ALL", {"left": 1, "right": 3}),
    ]
