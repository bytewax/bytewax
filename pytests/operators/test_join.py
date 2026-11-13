from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream
from bytewax.operators import JoinEmitMode, JoinInsertMode, _JoinState
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark, raises


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
    insert_mode: Optional[JoinInsertMode] = None,
    emit_mode: Optional[JoinEmitMode] = None,
) -> Dataflow:
    flow = Dataflow("test_df")
    lefts = op.input("inp_l", flow, TestingSource(inp_l))
    keyed_lefts = op.key_on("key_l", lefts, lambda _: "ALL")
    rights = op.input("inp_r", flow, TestingSource(inp_r))
    keyed_rights = op.key_on("key_r", rights, lambda _: "ALL")
    if insert_mode is not None and emit_mode is not None:
        joined = op.join(
            "join",
            keyed_lefts,
            keyed_rights,
            insert_mode=insert_mode,
            emit_mode=emit_mode,
        )
    else:
        joined = op.join(
            "join",
            keyed_lefts,
            keyed_rights,
        )
    unkeyed = op.key_rm("unkey", joined)
    op.output("out", unkeyed, TestingSink(out))
    return flow


def test_join_last_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "last", "complete")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_default_is_last_complete() -> None:
    inp_l = [1]
    inp_r = [2]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out)

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_first_final() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "first", "final")

    run_main(flow)
    assert out == [
        (1, 2),
    ]


def test_join_last_final() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "last", "final")

    run_main(flow)
    assert out == [
        (1, 3),
    ]


def test_join_last_running() -> None:
    inp_l = [1]
    inp_r = [2, 3]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "last", "running")

    run_main(flow)
    assert out == [
        (1, None),
        (1, 2),
        (1, 3),
    ]


def test_join_product_final() -> None:
    inp_l = [1, 2]
    inp_r = [3, 4]
    out: List[Tuple[Optional[int], Optional[int]]] = []

    flow = _build_join_dataflow(inp_l, inp_r, out, "product", "final")

    run_main(flow)
    assert out == [
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
    ]


# `(side, key, value)`. Key `a` gets two left values before its first
# right value, key `b` two right values after its first left value,
# and key `c` only ever a right value.
_MULTI_KEY_INP = [
    ("L", "a", 1),
    ("L", "a", 3),
    ("R", "a", 10),
    ("L", "b", 2),
    ("R", "b", 20),
    ("R", "b", 30),
    ("L", "a", 4),
    ("R", "c", 40),
]

_MULTI_KEY_EXPECTED: Dict[Tuple[str, str], Dict[str, List[Tuple]]] = {
    # Emit once every side has a value, then start over.
    ("first", "complete"): {"a": [(1, 10)], "b": [(2, 20)]},
    ("last", "complete"): {"a": [(3, 10)], "b": [(2, 20)]},
    ("product", "complete"): {"a": [(1, 10), (3, 10)], "b": [(2, 20)]},
    # Emit what was seen at EOF, with `None` for a side never seen.
    ("first", "final"): {"a": [(1, 10)], "b": [(2, 20)], "c": [(None, 40)]},
    ("last", "final"): {"a": [(4, 10)], "b": [(2, 30)], "c": [(None, 40)]},
    ("product", "final"): {
        "a": [(1, 10), (3, 10), (4, 10)],
        "b": [(2, 20), (2, 30)],
        "c": [(None, 40)],
    },
    # Emit what was seen so far after every value.
    ("first", "running"): {
        "a": [(1, None), (1, None), (1, 10), (1, 10)],
        "b": [(2, None), (2, 20), (2, 20)],
        "c": [(None, 40)],
    },
    ("last", "running"): {
        "a": [(1, None), (3, None), (3, 10), (4, 10)],
        "b": [(2, None), (2, 20), (2, 30)],
        "c": [(None, 40)],
    },
    ("product", "running"): {
        "a": [
            (1, None),
            (1, None),
            (3, None),
            (1, 10),
            (3, 10),
            (1, 10),
            (3, 10),
            (4, 10),
        ],
        "b": [(2, None), (2, 20), (2, 20), (2, 30)],
        "c": [(None, 40)],
    },
}


def _build_multi_key_join(
    inp: List[Any],
    out: List[Tuple[str, Tuple]],
    insert_mode: JoinInsertMode,
    emit_mode: JoinEmitMode,
) -> Dataflow:
    flow = Dataflow("test_df")
    s: Stream[Tuple[str, str, int]] = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", s, lambda x: x[1])
    b = op.branch("side", keyed, lambda kv: kv[1][0] == "L")
    lefts = op.map_value("left", b.trues, lambda x: x[2])
    rights = op.map_value("right", b.falses, lambda x: x[2])
    joined = op.join(
        "join", lefts, rights, insert_mode=insert_mode, emit_mode=emit_mode
    )
    op.output("out", joined, TestingSink(out))
    return flow


def _group_by_key(out: List[Tuple[str, Tuple]]) -> Dict[str, List[Tuple]]:
    # Rows for different keys may interleave in any order.
    groups: Dict[str, List[Tuple]] = defaultdict(list)
    for k, row in out:
        groups[k].append(row)
    return dict(groups)


_MODES = list(_MULTI_KEY_EXPECTED)


@mark.parametrize("insert_mode,emit_mode", _MODES)
def test_join_multiple_keys(
    insert_mode: JoinInsertMode, emit_mode: JoinEmitMode
) -> None:
    out: List[Tuple[str, Tuple]] = []
    flow = _build_multi_key_join(_MULTI_KEY_INP, out, insert_mode, emit_mode)

    run_main(flow)
    assert _group_by_key(out) == _MULTI_KEY_EXPECTED[(insert_mode, emit_mode)]


@mark.parametrize("insert_mode,emit_mode", _MODES)
def test_join_multiple_keys_resume_after_abort(
    tmp_path: Path, insert_mode: JoinInsertMode, emit_mode: JoinEmitMode
) -> None:
    expected = _MULTI_KEY_EXPECTED[(insert_mode, emit_mode)]
    for i in range(1, len(_MULTI_KEY_INP)):
        inp = _MULTI_KEY_INP[:i] + [TestingSource.ABORT()] + _MULTI_KEY_INP[i:]
        out: List[Tuple[str, Tuple]] = []
        flow = _build_multi_key_join(inp, out, insert_mode, emit_mode)
        db_dir = tmp_path / f"abort_{i}"
        db_dir.mkdir()
        init_db_dir(db_dir, 1)
        recovery_config = RecoveryConfig(str(db_dir))

        # Snapshot after every item so the resume continues right
        # after the abort.
        run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)
        run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)

        assert _group_by_key(out) == expected, f"abort before item {i}"


def test_join_rejects_unknown_insert_mode() -> None:
    flow = Dataflow("test_df")
    s = op.key_on("key", op.input("inp", flow, TestingSource([1])), str)

    with raises(ValueError, match="unknown join insert mode 'bogus'"):
        op.join("join", s, s, insert_mode="bogus")  # type: ignore[call-overload]


def test_join_rejects_unknown_emit_mode() -> None:
    flow = Dataflow("test_df")
    s = op.key_on("key", op.input("inp", flow, TestingSource([1])), str)

    with raises(ValueError, match="unknown join emit mode 'bogus'"):
        op.join("join", s, s, emit_mode="bogus")  # type: ignore[call-overload]
