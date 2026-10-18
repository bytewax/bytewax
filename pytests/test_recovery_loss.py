"""Crash/resume zero-loss matrix for `RecoveryConfig(snapshot_interval=...)`.

Each scenario crashes a dataflow partway through, resumes it until it
completes, and checks three invariants over everything the sink
collected across all executions:

- Z (zero loss): every input item is emitted at least once.
- S (exactly-once state): every emitted running total equals the
  plain-Python prefix sum for its key, and the final totals equal a
  single fold over the input.
- D (bounded duplicates): only items at or after the point the last
  snapshot boundary before the crash resumes from may be emitted
  twice.

Epochs are driven by item counts. Every item takes longer to process
than the epoch interval, so each batch closes its own epoch. With `n`
epochs per snapshot, boundaries are the epochs that are multiples of
`n`, and a crash resumes from the last boundary at or before it.

"""

import time
from collections import Counter
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.run import request_graceful_drain
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import mark, param, raises

ITEMS = list(range(1, 21))
NUM_KEYS = 4
EPOCH_TD = timedelta(seconds=0.05)
# Longer than an epoch, so each processed batch ends its own epoch.
SLOW_SECONDS = 0.08


def _snapshot_interval(n):
    return EPOCH_TD * n


def _resume_index(abort_at, n, batch_size=1):
    """Index of the first item a resumed execution emits.

    Batch `k` (counting from 0) is emitted in epoch `k + 1`, and every
    batch closes its own epoch. The abort is raised in epoch
    `abort_at // batch_size + 2`, and a snapshot written in the
    activation that raises the abort is lost. So the last surviving
    boundary is the largest multiple of `n` at or before epoch
    `abort_at // batch_size + 1`. Resume starts at the items after that
    boundary. Boundary `0` has no usable snapshot, so resume replays
    from the start.

    """
    boundary = (abort_at // batch_size + 1) // n * n
    return min(boundary * batch_size, abort_at)


def _keyed(item):
    return (f"key{item % NUM_KEYS}", item)


def _ground_truth(items):
    """Per-item running total and final per-key total, in plain Python."""
    running = {}
    prefix = {}
    for item in items:
        key = f"key{item % NUM_KEYS}"
        running[key] = running.get(key, 0) + item
        prefix[item] = running[key]
    return prefix, running


def _source(items, sentinels, batch_size=1):
    """Keyed items with `sentinels[i]` spliced in right before item `i`.

    Sentinel objects are reused by every execution, so ABORT triggers
    only once, as it does for the existing tests.

    """
    raw = []
    for index, item in enumerate(items):
        if index in sentinels:
            raw.append(sentinels[index])
        raw.append(_keyed(item))
    return TestingSource(raw, batch_size=batch_size)


def _slow_running_sum(state, item):
    time.sleep(SLOW_SECONDS)
    total = (state or 0) + item
    return (total, (item, total))


def _sum_flow(source, out):
    flow = Dataflow("test_df")
    s = op.input("inp", flow, source)
    s = op.stateful_map("sum", s, _slow_running_sum)
    op.output("out", s, TestingSink(out))
    return flow


def _execute(flow, recovery_config, out, pair_runs):
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    pair_runs.append(list(out))
    out.clear()


def _emitted_items(run):
    return [item for _key, (item, _value) in run]


def _check_zero_loss(item_runs, items):
    emitted = Counter(item for run in item_runs for item in run)
    assert set(emitted) >= set(items)
    return emitted


def _check_exactly_once_state(pair_runs, items):
    prefix, final_totals = _ground_truth(items)
    for run in pair_runs:
        for _key, (item, total) in run:
            assert total == prefix[item]
    final = {}
    for run in pair_runs:
        for key, (_item, total) in run:
            final[key] = total
    assert final == final_totals


def _check_bounded_duplicates(emitted, items, replay_from):
    position = {item: index for index, item in enumerate(items)}
    duplicated = [item for item, count in emitted.items() if count > 1]
    assert all(position[item] >= replay_from for item in duplicated)


def _check_model(emitted, items, spans, replay_from):
    """Exact emission counts from the model, plus the D bound.

    `spans` is one `(start, stop)` index pair per execution, the items
    that execution should emit.

    """
    _check_bounded_duplicates(emitted, items, replay_from)
    expected = Counter()
    for start, stop in spans:
        expected.update(items[start:stop])
    assert emitted == expected


def _check_sum_runs(pair_runs, items, spans, replay_from):
    item_runs = [_emitted_items(run) for run in pair_runs]
    emitted = _check_zero_loss(item_runs, items)
    _check_exactly_once_state(pair_runs, items)
    _check_model(emitted, items, spans, replay_from)


@mark.parametrize(
    "abort_at",
    [
        param(2, id="before-first-boundary"),
        param(7, id="right-after-boundary"),
        param(5, id="mid-window"),
        param(8, id="one-epoch-before-boundary"),
    ],
)
def test_crash_positions_n3(tmp_path, abort_at):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(3)
    )

    out = []
    pair_runs = []
    flow = _sum_flow(_source(ITEMS, {abort_at: TestingSource.ABORT()}), out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    replay_from = _resume_index(abort_at, 3)
    spans = [(0, abort_at), (replay_from, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    _check_sum_runs(pair_runs, ITEMS, spans, replay_from)


@mark.parametrize(
    ("n", "abort_at"),
    [
        param(1, 7, id="n1-every-epoch"),
        param(10, 15, id="n10-mid-window"),
    ],
)
def test_snapshot_interval_sizes(tmp_path, n, abort_at):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(n)
    )

    out = []
    pair_runs = []
    flow = _sum_flow(_source(ITEMS, {abort_at: TestingSource.ABORT()}), out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    replay_from = _resume_index(abort_at, n)
    spans = [(0, abort_at), (replay_from, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    _check_sum_runs(pair_runs, ITEMS, spans, replay_from)


def test_eof_window_crash_replays_from_start(tmp_path):
    init_db_dir(tmp_path, 1)
    # Longer than the whole stream, so only epoch 0 is a boundary.
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=timedelta(seconds=3600)
    )

    abort_at = 10
    out = []
    pair_runs = []
    flow = _sum_flow(_source(ITEMS, {abort_at: TestingSource.ABORT()}), out)
    _execute(flow, recovery_config, out, pair_runs)

    # The crash resumes from the epoch 0 boundary, which is the start.
    replay_from = _resume_index(abort_at, 72000)
    assert replay_from == 0

    # A clean run to EOF, then a resume attempt that must be empty.
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    assert pair_runs[-1] == []

    spans = [(0, abort_at), (0, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    _check_sum_runs(pair_runs, ITEMS, spans, replay_from)


def test_eof_window_clean_eof_then_resume_is_empty(tmp_path):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=timedelta(seconds=3600)
    )

    # Mirrors `test_snapshot_interval_eof_snapshots`, with a slow stream.
    out = []
    pair_runs = []
    flow = _sum_flow(_source(ITEMS, {10: TestingSource.EOF()}), out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    assert _emitted_items(pair_runs[0]) == ITEMS[:10]
    assert _emitted_items(pair_runs[1]) == ITEMS[10:]
    assert pair_runs[2] == []
    # A clean EOF snapshots the exact position, so nothing is replayed.
    spans = [(0, 10), (10, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    _check_sum_runs(pair_runs, ITEMS, spans, len(ITEMS))


def test_double_crash_resumes_from_each_boundary(tmp_path):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(3)
    )

    first, second = 8, 14
    sentinels = {
        first: TestingSource.ABORT(),
        second: TestingSource.ABORT(),
    }
    out = []
    pair_runs = []
    flow = _sum_flow(_source(ITEMS, sentinels), out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    assert pair_runs[-1] == []

    item_runs = [_emitted_items(run) for run in pair_runs]
    emitted = _check_zero_loss(item_runs, ITEMS)
    _check_exactly_once_state(pair_runs, ITEMS)

    # The first resume replays from the boundary before the first abort.
    replay_from = _resume_index(first, 3)
    assert item_runs[1] == ITEMS[replay_from:second]
    _check_bounded_duplicates(emitted, ITEMS, replay_from)

    # The third execution resumes at or after the boundary before the
    # second abort, and then runs to the end of the stream.
    third_start = ITEMS.index(item_runs[2][0])
    assert third_start >= _resume_index(second, 3)
    assert item_runs[2] == ITEMS[third_start:]


def _crash_once(value, crashed):
    def mapper(pair):
        time.sleep(SLOW_SECONDS)
        if pair[1] == value and not crashed[0]:
            crashed[0] = True
            msg = "simulated crash"
            raise RuntimeError(msg)
        return pair

    return mapper


def _error_text(error):
    """Messages from an exception and everything it wraps."""
    texts = []
    while error is not None:
        texts.append(str(error))
        error = error.__cause__ or error.__context__
    return " ".join(texts)


def test_multi_worker_crash_mid_window_loses_nothing(tmp_path):
    n = 3
    init_db_dir(tmp_path, 2)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(n)
    )

    crash_index = 12
    crashed = [False]
    out = []
    pair_runs = []
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([_keyed(item) for item in ITEMS]))
    s = op.map("crash_once", s, _crash_once(ITEMS[crash_index], crashed))
    s = op.stateful_map("sum", s, _slow_running_sum)
    op.output("out", s, TestingSink(out))

    def cluster():
        cluster_main(
            flow,
            addresses=[],
            proc_id=0,
            epoch_interval=EPOCH_TD,
            recovery_config=recovery_config,
            worker_count_per_proc=2,
        )

    with raises(RuntimeError) as excinfo:
        cluster()
    assert "simulated crash" in _error_text(excinfo.value)
    pair_runs.append(list(out))
    out.clear()

    cluster()
    pair_runs.append(list(out))
    out.clear()
    cluster()
    pair_runs.append(list(out))
    assert pair_runs[-1] == []

    # Two workers do not share one epoch timeline, so only Z and S are
    # exact. Each item here closes its own epoch, so every duplicate
    # must fall within the last 2 * n items before the crash.
    item_runs = [_emitted_items(run) for run in pair_runs]
    emitted = _check_zero_loss(item_runs, ITEMS)
    _check_exactly_once_state(pair_runs, ITEMS)
    window_start = crash_index - 2 * n
    for item, count in emitted.items():
        if count > 1:
            assert window_start <= ITEMS.index(item) < crash_index


def _slow_double(pair):
    time.sleep(SLOW_SECONDS)
    key, item = pair
    return (key, (item, item * 2))


def test_stateless_map_crash_mid_window_loses_nothing(tmp_path):
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(3)
    )

    abort_at = 8
    out = []
    pair_runs = []
    flow = Dataflow("test_df")
    s = op.input("inp", flow, _source(ITEMS, {abort_at: TestingSource.ABORT()}))
    s = op.map("double", s, _slow_double)
    op.output("out", s, TestingSink(out))
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    # Stateless: each output is its input doubled, and nothing else.
    for run in pair_runs:
        for _key, (item, doubled) in run:
            assert doubled == item * 2

    replay_from = _resume_index(abort_at, 3)
    spans = [(0, abort_at), (replay_from, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    item_runs = [_emitted_items(run) for run in pair_runs]
    emitted = _check_zero_loss(item_runs, ITEMS)
    _check_model(emitted, ITEMS, spans, replay_from)


@mark.parametrize("abort_at", range(1, len(ITEMS)))
def test_abort_sweep_every_position(tmp_path, abort_at):
    # Two items per batch, and each batch closes its own epoch.
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(3)
    )

    out = []
    pair_runs = []
    source = _source(ITEMS, {abort_at: TestingSource.ABORT()}, batch_size=2)
    flow = _sum_flow(source, out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    replay_from = _resume_index(abort_at, 3, batch_size=2)
    spans = [(0, abort_at), (replay_from, len(ITEMS)), (len(ITEMS), len(ITEMS))]
    _check_sum_runs(pair_runs, ITEMS, spans, replay_from)


def test_resumed_boundary_abort_stays_within_window(tmp_path):
    # Guarantee: the first batch of each execution closes its own epoch,
    # so epochs stay aligned with item counts across resumes. Here the
    # second abort (after item index 5) lands on boundary epoch 6, so the
    # resume from that boundary starts at index 6. Nothing below index 6
    # may be emitted twice.
    items = list(range(1, 9))
    init_db_dir(tmp_path, 1)
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=_snapshot_interval(3)
    )

    first, second = 4, 6
    sentinels = {
        first: TestingSource.ABORT(),
        second: TestingSource.ABORT(),
    }
    out = []
    pair_runs = []
    flow = _sum_flow(_source(items, sentinels), out)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)
    _execute(flow, recovery_config, out, pair_runs)

    item_runs = [_emitted_items(run) for run in pair_runs]
    _check_zero_loss(item_runs, items)
    _check_exactly_once_state(pair_runs, items)

    # Items replayed by the second resume must sit at or after index 6,
    # the naive boundary before the second abort.
    position = {item: index for index, item in enumerate(items)}
    replayed_twice = set(item_runs[1]) & set(item_runs[2])
    assert all(position[item] >= second for item in replayed_twice)


# The sink column of this matrix was `TestingSink`, which is stateless:
# its writes need no snapshot, so the output operator's own snapshots
# were never exercised. The scenarios below add a stateful sink. It
# holds writes until a snapshot publishes them, so anything the output
# operator fails to snapshot before a stop is lost.


class _BufferedSink(FixedPartitionedSink):
    def __init__(self, out):
        self._out = out

    def list_parts(self):
        return ["buffered"]

    def build_part(self, step_id, for_part, resume_state):
        return _BufferedPart(self._out)


class _BufferedPart(StatefulSinkPartition):
    def __init__(self, out):
        self._out = out
        self._buf = []

    def write_batch(self, values):
        self._buf.extend(values)

    def snapshot(self):
        self._out.extend(self._buf)
        self._buf = []
        return None


def _buffered_flow(items, out, drain_at=None):
    drained = [False]

    def tag(item):
        time.sleep(SLOW_SECONDS)
        if item == drain_at and not drained[0]:
            drained[0] = True
            request_graceful_drain()
        return ("buffered", item)

    flow = Dataflow("buffered_df")
    s = op.input("inp", flow, TestingSource(items))
    s = op.map("tag", s, tag)
    op.output("out", s, _BufferedSink(out))
    return flow


def _buffered_recovery(tmp_path):
    init_db_dir(tmp_path, 1)
    return RecoveryConfig(str(tmp_path), snapshot_interval=_snapshot_interval(3))


@mark.parametrize(
    "drain_at",
    [
        param(4, id="first-item-after-boundary"),
        param(5, id="mid-window"),
        param(7, id="mid-window-late"),
        param(8, id="last-item-of-window"),
    ],
)
def test_stateful_sink_drain_loses_nothing(tmp_path, drain_at):
    items = list(range(1, 11))
    recovery_config = _buffered_recovery(tmp_path)
    out = []
    flow = _buffered_flow(items, out, drain_at=drain_at)

    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    assert out == items[: len(out)]
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)

    # Resume replays nothing, so the stateful sink sees each item once.
    assert out == items


@mark.parametrize(
    "count",
    [
        param(10, id="mid-window"),
        param(11, id="one-past-boundary"),
        param(13, id="mid-window-late"),
    ],
)
def test_stateful_sink_eof_mid_window_loses_nothing(tmp_path, count):
    items = list(range(1, count + 1))
    recovery_config = _buffered_recovery(tmp_path)
    out = []
    flow = _buffered_flow(items, out)

    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    assert out == items
    run_main(flow, epoch_interval=EPOCH_TD, recovery_config=recovery_config)
    assert out == items
