from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import bytewax.operators as op
import bytewax.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.windowing import (
    LATE_SESSION_ID,
    SessionWindower,
    SystemClock,
    WindowMetadata,
    _session_find_merges,
    _SessionWindowerLogic,
    _SessionWindowerState,
)


def test_initial_session() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    found = logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_forward_within_gap() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))

    found = logic.open_for(datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
    )


def test_extend_forward_exact_gap() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))

    found = logic.open_for(datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
    )


def test_extend_backward_within_gap() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))

    found = logic.open_for(datetime(2024, 1, 1, 8, 59, 55, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 8, 59, 55, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_backward_exact_gap() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))

    found = logic.open_for(datetime(2024, 1, 1, 8, 59, 50, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 8, 59, 50, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
    )


def test_extend_merge() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10), state=_SessionWindowerState()
    )

    logic.open_for(datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc))
    logic.open_for(datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc))

    found = logic.open_for(
        datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
    )
    assert list(found) == [0]
    assert logic.merged() == [(1, 0)]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        merged_ids={1},
    )


def test_within_existing() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10),
        state=_SessionWindowerState(
            max_key=0,
            sessions={
                0: WindowMetadata(
                    open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
                    close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
                )
            },
        ),
    )

    found = logic.open_for(datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc))
    assert list(found) == [0]

    assert logic.state.sessions[0] == WindowMetadata(
        open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
    )


def test_late() -> None:
    logic = _SessionWindowerLogic(
        gap=timedelta(seconds=10),
        state=_SessionWindowerState(),
    )

    found = logic.late_for(datetime(2023, 12, 1, 9, 0, 0, tzinfo=timezone.utc))
    assert list(found) == [LATE_SESSION_ID]


def test_find_merges_none() -> None:
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == []
    assert sessions == sessions


def test_find_merges_within_gap() -> None:
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
            merged_ids={1},
        ),
    }


def test_find_merges_exact_gap() -> None:
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            merged_ids={1},
        ),
    }


def test_find_merges_multi() -> None:
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 5, tzinfo=timezone.utc),
        ),
        2: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(1, 0), (2, 0)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 10, tzinfo=timezone.utc),
            merged_ids={1, 2},
        ),
    }


def test_find_merges_no_yes_no() -> None:
    sessions = {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
        ),
        2: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
        ),
        3: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
        ),
    }

    assert _session_find_merges(sessions, timedelta(seconds=10)) == [(2, 1)]
    assert sessions == {
        0: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc),
        ),
        1: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 20, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 25, tzinfo=timezone.utc),
            merged_ids={2},
        ),
        3: WindowMetadata(
            open_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
            close_time=datetime(2024, 1, 1, 9, 0, 40, tzinfo=timezone.utc),
        ),
    }


def test_session_with_system_clock() -> None:
    flow = Dataflow("test_df")
    nums = op.input("input", flow, TestingSource(range(10)))
    keyed_nums = op.key_on("key", nums, lambda _: "ALL")

    def folder(s, v):
        s.append(v)
        return s

    fold_out = win.fold_window(
        "collect_records",
        keyed_nums,
        SystemClock(),
        SessionWindower(gap=timedelta(seconds=10)),
        list,
        folder,
        list.__add__,
    )
    unkeyed = op.key_rm("unkey", fold_out.down)

    out: List[Tuple[int, List[int]]] = []
    op.output("out", unkeyed, TestingSink(out))

    run_main(flow)

    assert out == [(0, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])]
