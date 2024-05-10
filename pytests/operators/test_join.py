from typing import Dict, List, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import JoinMode, _JoinState
from bytewax.testing import TestingSink, TestingSource, run_main


def test_join_state_astuples() -> None:
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


def test_join_state_asdicts() -> None:
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


def build_join_dataflow(
    inp_l: List[int],
    inp_r: List[int],
    out: List[Tuple[int, int]],
    mode: JoinMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _x: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _x: "ALL")
    joined = op.join("join", keyed_lefts, keyed_rights, mode=mode)
    cleaned = op.map("clean", joined, lambda x: x[1])
    op.output("out", cleaned, TestingSink(out))
    return flow


def test_join_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Tuple[int, int]] = []

    flow = build_join_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_final() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[int, int]] = []

    flow = build_join_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        (1, 3),
    ]


def test_join_running() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[int, int]] = []

    flow = build_join_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        (1, None),
        (1, 2),
        (1, 3),
    ]


def test_join_product() -> None:
    inp_l = [1, 2]
    inp_r = [3, 4]
    out: List[Tuple[int, int]] = []

    flow = build_join_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]


def build_join_named_dataflow(
    inp_l: List[int],
    inp_r: List[int],
    out: List[Dict[str, int]],
    mode: JoinMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _x: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _x: "ALL")
    joined = op.join_named("join", mode, left=keyed_lefts, right=keyed_rights)
    cleaned = op.map("clean", joined, lambda x: x[1])
    op.output("out", cleaned, TestingSink(out))
    return flow


def test_join_named_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Dict[str, int]] = []

    flow = build_join_named_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 2},
    ]


def test_join_named_final() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Dict[str, int]] = []

    flow = build_join_named_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 3},
    ]


def test_join_named_running() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Dict[str, int]] = []

    flow = build_join_named_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        {"left": 1},
        {"left": 1, "right": 2},
        {"left": 1, "right": 3},
    ]


def test_join_named_product() -> None:
    inp_l = [1, 2]
    inp_r = [3, 4]
    out: List[Dict[str, int]] = []

    flow = build_join_named_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        {"left": 1, "right": 3},
        {"left": 1, "right": 4},
        {"left": 2, "right": 3},
        {"left": 2, "right": 4},
    ]
