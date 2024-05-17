from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import JoinMode, _JoinState
from bytewax.testing import TestingSink, TestingSource, run_main


def test_join_state_astuples() -> None:
    state = _JoinState.for_side_count(3)
    state.add_val(0, 1)
    state.add_val(0, 2)
    state.add_val(2, 3)
    state.add_val(2, 4)

    assert list(state.astuples()) == [
        (1, None, 3),
        (1, None, 4),
        (2, None, 3),
        (2, None, 4),
    ]


def _build_join_dataflow(
    inp_l: List[int],
    inp_r: List[int],
    out: List[Tuple[Optional[int], Optional[int]]],
    mode: Optional[JoinMode] = None,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _: "ALL")
    if mode is not None:
        joined = op.join("join", keyed_lefts, keyed_rights, mode=mode)
    else:
        joined = op.join("join", keyed_lefts, keyed_rights)
    unkeyed = op.key_rm("unkey", joined)
    op.output("out", unkeyed, TestingSink(out))
    return flow


def test_join_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "complete")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_default_is_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out)

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_final() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "final")

    run_main(flow)
    assert out == [
        (1, 3),
    ]


def test_join_running() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "running")

    run_main(flow)
    assert out == [
        (1, None),
        (1, 2),
        (1, 3),
    ]


def test_join_product() -> None:
    inp_l = [1, 2]
    inp_r = [3, 4]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "product")

    run_main(flow)
    assert out == [
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]
