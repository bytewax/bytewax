"""Operator results don't depend on batching, workers, or resumes.

The tests for each operator run one worker with one item per batch.
These run dataflows over a longer input with several batch sizes and
worker counts, with and without crashes and resumes, and compare the
output with a plain Python model of each operator, or for windowing
operators with a run of one worker and one item per batch.

Only what is defined independent of arrival order is compared. Values
from different streams, like the sides of a join, have no defined
order between them, so `join` with complete or running emits is left
out, `join_window` only gets values that aren't late, and windows
with `ordered=False` are compared as sets of values.

"""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache, partial
from itertools import product
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow, Stream
from bytewax.errors import BytewaxRuntimeError
from bytewax.operators import (
    JoinEmitMode,
    JoinInsertMode,
    StatefulBatchLogic,
    StatefulLogic,
)
from bytewax.operators.windowing import (
    EventClock,
    SessionWindower,
    SlidingWindower,
    TumblingWindower,
    Windower,
    WindowMetadata,
    WindowOut,
)
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import mark, raises

N = 300
KEY_COUNT = 7
INP = list(range(N))

# Batches of 8 or more hold a key twice, which some operators handle
# within the batch.
BATCH_SIZES = [1, 2, 3, 8, 1000]
WORKER_COUNTS = [1, 2, 3]


def _key(x: int) -> str:
    return f"k{x % KEY_COUNT}"


def _by_key(xs: Iterable[int]) -> Dict[str, List[int]]:
    groups: Dict[str, List[int]] = defaultdict(list)
    for x in xs:
        groups[_key(x)].append(x)
    return groups


@dataclass(frozen=True)
class _Scenario:
    build: Callable[[Stream[int]], Stream[Any]]
    # Model of the output for the input `INP`. `None` means compare
    # with a run of one worker and one item per batch instead.
    model: Optional[Callable[[], List[Any]]]
    # Whether the output is `(key, value)` pairs whose order within
    # each key is part of the operator's contract.
    keyed_order: bool


def _stateless(s: Stream[int]) -> Stream[Any]:
    s = op.map("map", s, lambda x: x * 2)
    s = op.filter("filter", s, lambda x: x % 3 != 0)
    s = op.flat_map("flat_map", s, lambda x: [x, x + 1])
    s = op.filter_map("filter_map", s, lambda x: x if x % 4 else None)
    s = op.flat_map_batch("flat_map_batch", s, lambda xs: [x + 1 for x in xs])
    lists = op.map("wrap", s, lambda x: [x])
    s = op.flatten("flatten", lists)
    return op.inspect("inspect", s, lambda _step_id, _x: None)


def _stateless_model() -> List[Any]:
    out = []
    for x in INP:
        doubled = x * 2
        if doubled % 3 == 0:
            continue
        for y in (doubled, doubled + 1):
            if y % 4:
                out.append(y + 1)
    return out


def _keyed_stateless(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    ks = op.map_value("map_value", ks, lambda x: x * 3)
    ks = op.filter_value("filter_value", ks, lambda x: x % 2 == 0)
    ks = op.flat_map_value("flat_map_value", ks, lambda x: [x, -x])
    return op.filter_map_value("filter_map_value", ks, lambda x: x if x % 5 else None)


def _keyed_stateless_model() -> List[Any]:
    out = []
    for x in INP:
        y = x * 3
        if y % 2 != 0:
            continue
        for z in (y, -y):
            if z % 5:
                out.append((_key(x), z))
    return out


def _branch_merge(s: Stream[int]) -> Stream[Any]:
    b = op.branch("branch", s, lambda x: x % 2 == 0)
    evens = op.map("evens", b.trues, lambda x: ("even", x))
    odds = op.map("odds", b.falses, lambda x: ("odd", x))
    merged = op.merge("merge", evens, odds)
    return op.redistribute("redistribute", merged)


def _branch_merge_model() -> List[Any]:
    return [("even" if x % 2 == 0 else "odd", x) for x in INP]


def _running_sum(state: Optional[int], x: int) -> Tuple[Optional[int], int]:
    total = (state or 0) + x
    # Discard the state every time the sum passes 1000, so the next
    # item for the key starts a new sum.
    return (None if total > 1000 else total, total)


def _stateful_map(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    return op.stateful_map("stateful_map", ks, _running_sum)


def _stateful_map_model() -> List[Any]:
    out = []
    for k, xs in _by_key(INP).items():
        state: Optional[int] = None
        for x in xs:
            state, total = _running_sum(state, x)
            out.append((k, total))
    return out


def _every_third(
    count: Optional[int], x: int
) -> Tuple[Optional[int], List[Tuple[int, int]]]:
    count = (count or 0) + 1
    return (count, [(count, x)] if count % 3 == 0 else [])


def _stateful_flat_map(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    return op.stateful_flat_map("stateful_flat_map", ks, _every_third)


def _stateful_flat_map_model() -> List[Any]:
    out: List[Any] = []
    for k, xs in _by_key(INP).items():
        count: Optional[int] = None
        for x in xs:
            count, ys = _every_third(count, x)
            out.extend((k, y) for y in ys)
    return out


class _SumLogic(StatefulLogic[int, Tuple[str, int], int]):
    def __init__(self, total: int) -> None:
        self.total = total

    def on_item(self, value: int) -> Tuple[Iterable[Tuple[str, int]], bool]:
        self.total += value
        return ([("item", self.total)], self.total > 1000)

    def on_eof(self) -> Tuple[Iterable[Tuple[str, int]], bool]:
        return ([("eof", self.total)], StatefulLogic.RETAIN)

    def snapshot(self) -> int:
        return self.total


def _build_sum(resume: Optional[int]) -> StatefulLogic[int, Tuple[str, int], int]:
    return _SumLogic(resume or 0)


def _stateful(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    return op.stateful("stateful", ks, _build_sum)


def _stateful_model() -> List[Any]:
    out = []
    for k, xs in _by_key(INP).items():
        total = 0
        alive = False
        for x in xs:
            total += x
            alive = True
            out.append((k, ("item", total)))
            if total > 1000:
                total = 0
                alive = False
        # A discarded logic is not called on EOF.
        if alive:
            out.append((k, ("eof", total)))
    return out


class _BatchSumLogic(StatefulBatchLogic[int, int, int]):
    def __init__(self, total: int) -> None:
        self.total = total

    def on_batch(self, values: List[int]) -> Tuple[Iterable[int], bool]:
        out = []
        for value in values:
            self.total += value
            out.append(self.total)
        return (out, StatefulBatchLogic.RETAIN)

    def snapshot(self) -> int:
        return self.total


def _build_batch_sum(resume: Optional[int]) -> StatefulBatchLogic[int, int, int]:
    return _BatchSumLogic(resume or 0)


def _stateful_batch(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    return op.stateful_batch("stateful_batch", ks, _build_batch_sum)


def _stateful_batch_model() -> List[Any]:
    out = []
    for k, xs in _by_key(INP).items():
        total = 0
        for x in xs:
            total += x
            out.append((k, total))
    return out


def _collect(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    return op.collect("collect", ks, timeout=timedelta(days=1), max_size=4)


def _collect_model() -> List[Any]:
    out: List[Any] = []
    for k, xs in _by_key(INP).items():
        out.extend((k, xs[i : i + 4]) for i in range(0, len(xs), 4))
    return out


def _finals(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    folded = op.fold_final("fold_final", ks, list, lambda acc, x: acc + [x])
    lists = op.map_value("wrap", ks, lambda x: [x])
    reduced = op.reduce_final("reduce_final", lists, lambda a, b: a + b)
    counted = op.count_final("count_final", s, _key)
    maxed = op.max_final("max_final", ks)
    # With `by=-x` the minimum is the largest value.
    mined = op.min_final("min_final", ks, by=lambda x: -x)
    return op.merge(
        "merge",
        op.map_value("tag_fold", folded, lambda v: ("fold", v)),
        op.map_value("tag_reduce", reduced, lambda v: ("reduce", v)),
        op.map_value("tag_count", counted, lambda v: ("count", v)),
        op.map_value("tag_max", maxed, lambda v: ("max", v)),
        op.map_value("tag_min", mined, lambda v: ("min", v)),
    )


def _finals_model() -> List[Any]:
    out: List[Any] = []
    for k, xs in _by_key(INP).items():
        out.append((k, ("fold", xs)))
        out.append((k, ("reduce", xs)))
        out.append((k, ("count", len(xs))))
        out.append((k, ("max", max(xs))))
        out.append((k, ("min", max(xs))))
    return out


def _join_final(insert_mode: JoinInsertMode, s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    b = op.branch("branch", ks, lambda kv: kv[1] % 2 == 0)
    return op.join(
        "join",
        b.trues,
        b.falses,
        insert_mode=insert_mode,
        emit_mode="final",
    )


def _join_final_model(insert_mode: JoinInsertMode) -> List[Any]:
    out: List[Any] = []
    for k, xs in _by_key(INP).items():
        evens = [x for x in xs if x % 2 == 0]
        odds = [x for x in xs if x % 2 != 0]
        if insert_mode == "first":
            out.append((k, (evens[0], odds[0])))
        elif insert_mode == "last":
            out.append((k, (evens[-1], odds[-1])))
        else:
            out.extend((k, (e, o)) for e, o in product(evens, odds))
    return out


def _enrich_cached(s: Stream[int]) -> Stream[Any]:
    def getter(k: str) -> str:
        return k.upper()

    def mapper(cache: Any, x: int) -> Tuple[int, str]:
        return (x, cache.get(_key(x)))

    return op.enrich_cached("enrich_cached", s, getter, mapper)


def _enrich_cached_model() -> List[Any]:
    return [(x, _key(x).upper()) for x in INP]


ALIGN_TO = datetime(2022, 1, 1, tzinfo=timezone.utc)


def _secs(x: int) -> int:
    # Mostly increasing timestamps with a gap every 50 items (to end
    # sessions), some items out of order within the wait, and some
    # items late.
    secs = x + 20 * (x // 50)
    if x % 5 == 0:
        secs += 3
    if x % 11 == 0:
        secs -= 10
    return secs


def _clock() -> EventClock[int]:
    # Freeze system time so the watermark only depends on the items.
    return EventClock(
        lambda x: ALIGN_TO + timedelta(seconds=_secs(x)),
        wait_for_system_duration=timedelta(seconds=4),
        now_getter=lambda: ALIGN_TO,
        to_system_utc=lambda _: None,
    )


def _norm_meta(meta: WindowMetadata) -> Tuple[Any, ...]:
    return (meta.open_time, meta.close_time, tuple(sorted(meta.merged_ids)))


def _window_streams(wo: WindowOut) -> Stream[Any]:
    return op.merge(
        "merge",
        op.map_value("tag_down", wo.down, lambda v: ("down", v)),
        op.map_value("tag_late", wo.late, lambda v: ("late", v)),
        op.map_value("tag_meta", wo.meta, lambda v: ("meta", v[0], _norm_meta(v[1]))),
    )


def _collect_window_session(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    windower = SessionWindower(gap=timedelta(seconds=10))
    return _window_streams(win.collect_window("w", ks, _clock(), windower))


def _fold_window_tumbling(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    windower = TumblingWindower(length=timedelta(seconds=30), align_to=ALIGN_TO)
    wo = win.fold_window(
        "w", ks, _clock(), windower, list, lambda a, x: a + [x], list.__add__
    )
    return _window_streams(wo)


def _count_window_sliding(s: Stream[int]) -> Stream[Any]:
    windower = SlidingWindower(
        length=timedelta(seconds=30), offset=timedelta(seconds=10), align_to=ALIGN_TO
    )
    return _window_streams(win.count_window("w", s, _clock(), windower, _key))


def _sorted_down(wo: WindowOut) -> WindowOut:
    # With `ordered=False` the order of values in a window isn't
    # defined; compare their sorted contents.
    down = op.map_value("sort", wo.down, lambda id_xs: (id_xs[0], sorted(id_xs[1])))
    return WindowOut(down, wo.late, wo.meta)


def _collect_window_session_unordered(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    # With this gap some on-time values arrive between two sessions and
    # merge them, which only happens with `ordered=False`.
    windower = SessionWindower(gap=timedelta(seconds=5))
    wo = win.collect_window("w", ks, _clock(), windower, ordered=False)
    return _window_streams(_sorted_down(wo))


def _fold_window_tumbling_unordered(s: Stream[int]) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    windower = TumblingWindower(length=timedelta(seconds=30), align_to=ALIGN_TO)
    wo = win.fold_window(
        "w",
        ks,
        _clock(),
        windower,
        list,
        lambda a, x: a + [x],
        list.__add__,
        ordered=False,
    )
    return _window_streams(_sorted_down(wo))


def _join_window(
    insert_mode: JoinInsertMode,
    emit_mode: JoinEmitMode,
    windower: Windower[Any],
    s: Stream[int],
) -> Stream[Any]:
    ks = op.key_on("key", s, _key)
    b = op.branch("branch", ks, lambda kv: kv[1] % 2 == 0)
    # Wait long enough that no value is late: which values are late
    # depends on the order the sides arrive in, which isn't defined.
    # For the same reason, give every value its own timestamp (still
    # out of order): values with equal timestamps are applied in
    # arrival order. Windows then only close at EOF, so a crash
    # snapshots values still queued in the window, not join state;
    # test_window_recovery.py covers the join state.
    clock: EventClock[int] = EventClock(
        lambda x: ALIGN_TO + timedelta(seconds=(x * 37) % N),
        wait_for_system_duration=timedelta(days=1),
        now_getter=lambda: ALIGN_TO,
        to_system_utc=lambda _: None,
    )
    wo = win.join_window(
        "w",
        clock,
        windower,
        b.trues,
        b.falses,
        insert_mode=insert_mode,
        emit_mode=emit_mode,
    )
    return _window_streams(wo)


SCENARIOS: Dict[str, _Scenario] = {
    "stateless": _Scenario(_stateless, _stateless_model, False),
    "keyed_stateless": _Scenario(_keyed_stateless, _keyed_stateless_model, True),
    "branch_merge_redistribute": _Scenario(_branch_merge, _branch_merge_model, False),
    "stateful_map": _Scenario(_stateful_map, _stateful_map_model, True),
    "stateful_flat_map": _Scenario(_stateful_flat_map, _stateful_flat_map_model, True),
    "stateful": _Scenario(_stateful, _stateful_model, True),
    "stateful_batch": _Scenario(_stateful_batch, _stateful_batch_model, True),
    "collect": _Scenario(_collect, _collect_model, True),
    "finals": _Scenario(_finals, _finals_model, False),
    "join_final_first": _Scenario(
        partial(_join_final, "first"), partial(_join_final_model, "first"), False
    ),
    "join_final_last": _Scenario(
        partial(_join_final, "last"), partial(_join_final_model, "last"), False
    ),
    "join_final_product": _Scenario(
        partial(_join_final, "product"), partial(_join_final_model, "product"), False
    ),
    "enrich_cached": _Scenario(_enrich_cached, _enrich_cached_model, False),
    "collect_window_session": _Scenario(_collect_window_session, None, False),
    "fold_window_tumbling": _Scenario(_fold_window_tumbling, None, False),
    "count_window_sliding": _Scenario(_count_window_sliding, None, False),
    "collect_window_session_unordered": _Scenario(
        _collect_window_session_unordered, None, False
    ),
    "fold_window_tumbling_unordered": _Scenario(
        _fold_window_tumbling_unordered, None, False
    ),
    "join_window_tumbling_last_complete": _Scenario(
        partial(
            _join_window,
            "last",
            "complete",
            TumblingWindower(length=timedelta(seconds=30), align_to=ALIGN_TO),
        ),
        None,
        False,
    ),
    "join_window_session_first_final": _Scenario(
        partial(_join_window, "first", "final", SessionWindower(timedelta(seconds=10))),
        None,
        False,
    ),
    "join_window_sliding_product_running": _Scenario(
        partial(
            _join_window,
            "product",
            "running",
            SlidingWindower(
                length=timedelta(seconds=30),
                offset=timedelta(seconds=10),
                align_to=ALIGN_TO,
            ),
        ),
        None,
        False,
    ),
}


def _canon(items: List[Any], keyed_order: bool) -> Any:
    if keyed_order:
        groups: Dict[str, List[Any]] = defaultdict(list)
        for k, v in items:
            groups[k].append(v)
        return dict(groups)
    else:
        return sorted(items, key=repr)


def _run(
    scenario: _Scenario,
    batch_size: int,
    worker_count: int,
    out: List[Any],
    crash: Callable[[int], int] = lambda x: x,
    recovery_config: Optional[RecoveryConfig] = None,
) -> None:
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP, batch_size))
    s = op.map("crash", s, crash)
    op.output("out", scenario.build(s), TestingSink(out))
    kwargs: Dict[str, Any] = {
        "epoch_interval": timedelta(0),
        "recovery_config": recovery_config,
    }
    if worker_count == 1:
        run_main(flow, **kwargs)
    else:
        cluster_main(flow, [], 0, worker_count_per_proc=worker_count, **kwargs)


@lru_cache(maxsize=None)
def _expected(name: str) -> List[Any]:
    scenario = SCENARIOS[name]
    if scenario.model is not None:
        return scenario.model()
    else:
        expected: List[Any] = []
        _run(scenario, batch_size=1, worker_count=1, out=expected)
        return expected


@mark.parametrize("worker_count", WORKER_COUNTS)
@mark.parametrize("batch_size", BATCH_SIZES)
@mark.parametrize("name", list(SCENARIOS))
def test_output_independent_of_batching_and_workers(
    name: str, batch_size: int, worker_count: int
) -> None:
    scenario = SCENARIOS[name]
    out: List[Any] = []
    _run(scenario, batch_size, worker_count, out)
    expected = _expected(name)
    assert _canon(out, scenario.keyed_order) == _canon(expected, scenario.keyed_order)


def _root_cause(ex: BaseException) -> str:
    while ex.__cause__ is not None:
        ex = ex.__cause__
    return str(ex)


class _Crasher:
    """Fail the dataflow once at each given item.

    Also records the first item each execution sees, to check where
    it resumed.

    """

    def __init__(self, *xs: int) -> None:
        self._pending = set(xs)
        self.firsts: List[Optional[int]] = []

    def start_execution(self) -> None:
        self.firsts.append(None)

    def __call__(self, x: int) -> int:
        if self.firsts[-1] is None:
            self.firsts[-1] = x
        if x in self._pending:
            self._pending.remove(x)
            msg = f"crash at {x}"
            raise RuntimeError(msg)
        return x


# A crash inside the first batch can't resume past it, so the largest
# batch size here still snapshots before the first crash.
@mark.parametrize("worker_count", WORKER_COUNTS)
@mark.parametrize("batch_size", [1, 3, 64])
@mark.parametrize("name", list(SCENARIOS))
def test_output_complete_after_crashes_and_resumes(
    tmp_path: Path, name: str, batch_size: int, worker_count: int
) -> None:
    scenario = SCENARIOS[name]
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(str(tmp_path))
    # `TestingSource.ABORT` only works with one worker, so fail the
    # dataflow from a user function instead, like a real crash.
    crash = _Crasher(N // 3, 2 * N // 3)

    out: List[Any] = []
    for at in (N // 3, 2 * N // 3):
        crash.start_execution()
        with raises(BytewaxRuntimeError) as exc_info:
            _run(scenario, batch_size, worker_count, out, crash, recovery_config)
        assert _root_cause(exc_info.value) == f"crash at {at}"
    crash.start_execution()
    _run(scenario, batch_size, worker_count, out, crash, recovery_config)

    # Each execution resumed after the start of the previous one.
    first, second, third = crash.firsts
    assert first == 0
    assert second is not None and 0 < second <= N // 3
    assert third is not None and second < third <= 2 * N // 3

    # Recovery is at-least-once: output from the epochs that were
    # running when the dataflow failed may be emitted again, but
    # nothing may be missing and nothing else may appear.
    expected = _expected(name)
    missing = Counter(map(repr, expected)) - Counter(map(repr, out))
    assert not missing
    assert set(map(repr, out)) == set(map(repr, expected))


@mark.parametrize("worker_count", WORKER_COUNTS)
@mark.parametrize("batch_size", BATCH_SIZES)
def test_unordered_session_merges_keep_every_value(
    batch_size: int, worker_count: int
) -> None:
    # The comparisons above use a reference run, which can't catch a
    # merge that loses values in every run. Check directly that each
    # value ends up in exactly one session or is late.
    down: List[Any] = []
    late: List[Any] = []
    meta: List[Any] = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP, batch_size))
    ks = op.key_on("key", s, _key)
    wo = win.collect_window(
        "w", ks, _clock(), SessionWindower(gap=timedelta(seconds=5)), ordered=False
    )
    op.output("down", wo.down, TestingSink(down))
    op.output("late", wo.late, TestingSink(late))
    op.output("meta", wo.meta, TestingSink(meta))
    if worker_count == 1:
        run_main(flow)
    else:
        cluster_main(flow, [], 0, worker_count_per_proc=worker_count)

    assert any(m.merged_ids for _, (_, m) in meta)
    windowed = [x for _, (_, xs) in down for x in xs]
    late_values = [x for _, (_, x) in late]
    assert sorted(windowed + late_values) == INP
