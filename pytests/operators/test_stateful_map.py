import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import raises


def test_stateful_map():
    inp = [2, 5, 8, 1, 3]
    out = []

    def running_mean(last_3, new):
        if last_3 is None:
            last_3 = []
        last_3.append(new)
        if len(last_3) > 3:
            last_3 = last_3[:-3]
        avg = sum(last_3) / len(last_3)
        return (last_3, avg)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful_map("running_mean", s, running_mean)
    op.output("out", s, TestingSink(out))

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
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful_map("bad_mapper", s, bad_mapper)
    op.output("out", s, TestingSink(out))

    expect = "must be a 2-tuple"
    with raises(RuntimeError):
        with raises(TypeError, match=re.escape(expect)):
            run_main(flow)


@dataclass
class _Session:
    started: datetime
    values: List[int] = field(default_factory=list)
    last: Optional[int] = None
    counts: Dict[str, int] = field(default_factory=dict)


_ALIGN_TO = datetime(2022, 1, 1, tzinfo=timezone.utc)


def _track(
    state: Optional[_Session], x: Tuple[int, str]
) -> Tuple[_Session, Tuple[int, List[int], Optional[int], Dict[str, int]]]:
    secs, label = x
    if state is None:
        state = _Session(_ALIGN_TO + timedelta(seconds=secs))
    # Mutate the state in place, as user code often does.
    emitted = (
        int((_ALIGN_TO + timedelta(seconds=secs) - state.started).total_seconds()),
        list(state.values),
        state.last,
        dict(state.counts),
    )
    state.values.append(secs)
    state.last = secs
    state.counts[label] = state.counts.get(label, 0) + 1
    return (state, emitted)


_SESSION_INP = [(0, "x"), (5, "y"), (9, "x"), (12, "z"), (20, "x"), (21, "y")]


def _build_session_flow(inp: List, out: List) -> Dataflow:
    flow = Dataflow("test_df")
    s: Stream[Tuple[int, str]] = op.input("inp", flow, TestingSource(inp))
    keyed = op.key_on("key", s, lambda _: "ALL")
    op.output("out", op.stateful_map("track", keyed, _track), TestingSink(out))
    return flow


def test_stateful_map_dataclass_state_resumes(tmp_path: Path) -> None:
    expected: List = []
    run_main(_build_session_flow(_SESSION_INP, expected))

    for i in range(1, len(_SESSION_INP)):
        inp = _SESSION_INP[:i] + [TestingSource.ABORT()] + _SESSION_INP[i:]
        out: List = []
        flow = _build_session_flow(inp, out)
        db_dir = tmp_path / f"abort_{i}"
        db_dir.mkdir()
        init_db_dir(db_dir, 1)
        recovery_config = RecoveryConfig(str(db_dir))

        # Snapshot after every item, so the resume picks up the state
        # as it was right before the abort.
        run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)
        run_main(flow, epoch_interval=timedelta(0), recovery_config=recovery_config)

        assert out == expected, f"abort before item {i}"
