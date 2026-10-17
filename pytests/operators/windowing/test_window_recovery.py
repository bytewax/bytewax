"""Recovery round trips for the windowing operators.

Each test runs a windowing dataflow once without interruption and
again with the input aborted and resumed from the recovery store; the
two runs must emit the same items on every output stream.

"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow, Stream
from bytewax.operators import JoinEmitMode, JoinInsertMode, KeyedStream
from bytewax.operators.windowing import (
    ZERO_TD,
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
    Windower,
    WindowMetadata,
    WindowOut,
)
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark

ALIGN_TO = datetime(2022, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class _Event:
    key: str
    secs: int

    @property
    def timestamp(self) -> datetime:
        return ALIGN_TO + timedelta(seconds=self.secs)

    @property
    def side(self) -> int:
        return self.secs % 2


# Two keys with out-of-order items inside the 5 second wait, items
# that arrive late, and (in unordered mode) items that bridge two
# sessions so the session windower has to merge them.
EVENTS = [
    _Event("a", 0),
    _Event("b", 1),
    _Event("a", 8),
    # Bridges the `a` sessions at 0 and 8.
    _Event("a", 4),
    _Event("b", 9),
    _Event("b", 4),
    _Event("a", 12),
    _Event("b", 20),
    _Event("a", 25),
    # Late: the `b` watermark is at 15.
    _Event("b", 14),
    _Event("a", 21),
    # Late: the `a` watermark is at 20.
    _Event("a", 3),
    _Event("b", 21),
    _Event("a", 30),
    # Late: the `a` watermark is at 25.
    _Event("a", 19),
]


def _clock() -> EventClock[_Event]:
    # Freeze system time so the watermark only depends on the items
    # seen, and never ask the runtime to wake up, so window closes
    # depend only on items and EOF.
    return EventClock(
        lambda e: e.timestamp,
        wait_for_system_duration=timedelta(seconds=5),
        now_getter=lambda: ALIGN_TO,
        to_system_utc=lambda _: None,
    )


WINDOWERS: Dict[str, Callable[[], Windower[Any]]] = {
    "tumbling": lambda: TumblingWindower(
        length=timedelta(seconds=10), align_to=ALIGN_TO
    ),
    "sliding": lambda: SlidingWindower(
        length=timedelta(seconds=10), offset=timedelta(seconds=5), align_to=ALIGN_TO
    ),
    "session": lambda: SessionWindower(gap=timedelta(seconds=5)),
}

_Op = Callable[[KeyedStream[_Event], Windower], WindowOut]


def _append(acc: List[int], e: _Event) -> List[int]:
    acc.append(e.secs)
    return acc


def _concat(a: List[int], b: List[int]) -> List[int]:
    a.extend(b)
    return a


def _reduce(s: KeyedStream[_Event], windower: Windower) -> WindowOut:
    secs = op.map_value("secs", s, lambda e: (e.secs,))
    # Values are now tuples; give the clock a matching getter.
    clock: EventClock[Tuple[int, ...]] = EventClock(
        lambda t: ALIGN_TO + timedelta(seconds=t[-1]),
        wait_for_system_duration=timedelta(seconds=5),
        now_getter=lambda: ALIGN_TO,
        to_system_utc=lambda _: None,
    )
    return win.reduce_window("w", secs, clock, windower, lambda a, b: a + b)


def _join(insert_mode: JoinInsertMode, emit_mode: JoinEmitMode, ordered: bool) -> _Op:
    def build(s: KeyedStream[_Event], windower: Windower) -> WindowOut:
        b = op.branch("side", s, lambda kv: kv[1].side == 0)
        return win.join_window(
            "w",
            _clock(),
            windower,
            b.trues,
            b.falses,
            insert_mode=insert_mode,
            emit_mode=emit_mode,
            ordered=ordered,
        )

    return build


OPS: Dict[str, _Op] = {
    "collect": lambda s, w: win.collect_window("w", s, _clock(), w),
    "collect_unordered": lambda s, w: win.collect_window(
        "w", s, _clock(), w, ordered=False
    ),
    "fold": lambda s, w: win.fold_window("w", s, _clock(), w, list, _append, _concat),
    "fold_unordered": lambda s, w: win.fold_window(
        "w", s, _clock(), w, list, _append, _concat, ordered=False
    ),
    "reduce": _reduce,
    "max": lambda s, w: win.max_window("w", s, _clock(), w, by=lambda e: e.secs),
    "min": lambda s, w: win.min_window("w", s, _clock(), w, by=lambda e: e.secs),
    "join_final": _join("last", "final", True),
    "join_final_unordered": _join("last", "final", False),
    "join_first_unordered": _join("first", "final", False),
    "join_product_unordered": _join("product", "final", False),
    "join_complete": _join("last", "complete", True),
    "join_running_unordered": _join("last", "running", False),
}


def _norm_meta(meta: WindowMetadata) -> Tuple[Any, ...]:
    return (meta.open_time, meta.close_time, tuple(sorted(meta.merged_ids)))


def _build_flow(
    build_op: Optional[_Op],
    windower_name: str,
    inp: List[Any],
    out: List[Tuple[str, str, Any]],
    count: bool = False,
) -> Dataflow:
    flow = Dataflow("test_df")
    # Emit one item per batch. An abort ends the current batch, and
    # batch boundaries change the order items reach each window: the
    # join sides interleave differently, which changes which items are
    # late, and unordered windows merge in a different order. With
    # one-item batches the order is the same with and without aborts.
    events: Stream[_Event] = op.input("inp", flow, TestingSource(inp, batch_size=1))
    windower = WINDOWERS[windower_name]()
    if count:
        wo = win.count_window("w", events, _clock(), windower, lambda e: e.key)
    else:
        assert build_op is not None
        keyed = op.key_on("key", events, lambda e: e.key)
        wo = build_op(keyed, windower)

    down = op.map("tag_down", wo.down, lambda kv: ("down", kv[0], kv[1]))
    late = op.map("tag_late", wo.late, lambda kv: ("late", kv[0], kv[1]))
    meta = op.map(
        "tag_meta",
        wo.meta,
        lambda kv: ("meta", kv[0], (kv[1][0], _norm_meta(kv[1][1]))),
    )
    op.output("out", op.merge("merge", down, late, meta), TestingSink(out))
    return flow


def _canon(items: List[Any]) -> List[Any]:
    # Keys are processed in a different order across restarts, so
    # compare the emitted items as multisets.
    return sorted(items, key=repr)


def _new_recovery_config(path: Path) -> RecoveryConfig:
    path.mkdir()
    init_db_dir(path, 1)
    return RecoveryConfig(str(path))


def _run(flow: Dataflow, recovery_config: Optional[RecoveryConfig]) -> None:
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)


def _check_single_abort(
    tmp_path: Path, build_op: Optional[_Op], windower_name: str, count: bool
) -> None:
    expected: List[Any] = []
    _run(_build_flow(build_op, windower_name, EVENTS, expected, count), None)
    # Make sure the scenario exercises every output stream.
    assert {typ for typ, _, _ in expected} == {"down", "late", "meta"}

    for i in range(1, len(EVENTS)):
        inp = EVENTS[:i] + [TestingSource.ABORT()] + EVENTS[i:]
        out: List[Any] = []
        flow = _build_flow(build_op, windower_name, inp, out, count)
        recovery_config = _new_recovery_config(tmp_path / f"abort_{i}")

        _run(flow, recovery_config)
        _run(flow, recovery_config)

        assert _canon(out) == _canon(expected), f"abort before item {i}"


def _check_abort_after_every_item(
    tmp_path: Path, build_op: Optional[_Op], windower_name: str, count: bool
) -> None:
    expected: List[Any] = []
    _run(_build_flow(build_op, windower_name, EVENTS, expected, count), None)

    inp: List[Any] = []
    for event in EVENTS:
        inp.append(event)
        inp.append(TestingSource.ABORT())
    out: List[Any] = []
    flow = _build_flow(build_op, windower_name, inp, out, count)
    recovery_config = _new_recovery_config(tmp_path / "abort_every")

    # One run per abort, then a final run to EOF.
    for _ in range(len(EVENTS) + 1):
        _run(flow, recovery_config)

    assert _canon(out) == _canon(expected)


@mark.parametrize("windower_name", list(WINDOWERS))
@mark.parametrize("op_name", list(OPS))
def test_window_resume_after_abort(
    tmp_path: Path, op_name: str, windower_name: str
) -> None:
    _check_single_abort(tmp_path, OPS[op_name], windower_name, count=False)


@mark.parametrize("windower_name", list(WINDOWERS))
def test_count_window_resume_after_abort(tmp_path: Path, windower_name: str) -> None:
    _check_single_abort(tmp_path, None, windower_name, count=True)


@mark.parametrize("windower_name", list(WINDOWERS))
@mark.parametrize("op_name", list(OPS))
def test_window_resume_after_abort_after_every_item(
    tmp_path: Path, op_name: str, windower_name: str
) -> None:
    _check_abort_after_every_item(tmp_path, OPS[op_name], windower_name, count=False)


@mark.parametrize("windower_name", list(WINDOWERS))
def test_count_window_resume_after_abort_after_every_item(
    tmp_path: Path, windower_name: str
) -> None:
    _check_abort_after_every_item(tmp_path, None, windower_name, count=True)
