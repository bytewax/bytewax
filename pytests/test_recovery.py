import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import timedelta
from itertools import accumulate

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.recovery import (
    InconsistentPartitionsError,
    MissingPartitionsError,
    NoPartitionsError,
    RecoveryConfig,
    init_db_dir,
)
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import mark, raises

# Recovery stores use STRICT tables, which need SQLite 3.37. Python 3.8 on
# Windows bundles an older SQLite, so tests that read a store with
# `sqlite3` skip there.
requires_strict_tables = mark.skipif(
    sqlite3.sqlite_version_info < (3, 37, 0),
    reason=f"sqlite3 {sqlite3.sqlite_version} predates STRICT tables (3.37)",
)


ZERO_TD = timedelta(seconds=0)
FIVE_TD = timedelta(seconds=5)
TEN_TD = timedelta(seconds=10)


def test_abort_no_snapshots(recovery_config):
    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting the epoch interval to 5 sec means we shouldn't have a
    # snapshot when the abort happens.
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    # So resume should re-play all input.
    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2, 3, 4]


def test_abort_resumes_from_first_batch_snapshot(tmp_path):
    init_db_dir(tmp_path, 1)
    # Only a snapshot interval pre-ages the first epoch.
    recovery_config = RecoveryConfig(str(tmp_path), snapshot_interval=FIVE_TD)

    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # The epoch interval is 5 sec, but the first batch closes the
    # pre-aged first epoch, so a snapshot is written right after item 0.
    # An abort right after start therefore resumes from that point
    # instead of replaying from scratch.
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]
    first_run = list(out)

    # Resume starts at item 1, so item 0 is not replayed. Items 1 and 2
    # were in an epoch that never closed, so they are emitted again.
    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [1, 2, 3, 4]
    resumed = list(out)

    # At-least-once: every item is delivered, and item 0 exactly once.
    delivered = first_run + resumed
    assert set(delivered) == {0, 1, 2, 3, 4}
    assert delivered.count(0) == 1


def test_abort_with_snapshots(recovery_config):
    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting the epoch interval to 0 sec means we will have a
    # snapshot after each item.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    # We should resume as if it was an EOF.
    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]


def test_continuation(recovery_config):
    inp = [0, 1, 2, TestingSource.EOF(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting the epoch interval to 5 sec means we should only
    # snapshot on EOF.
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    # Each continuation should resume at the last snapshot.
    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [3, 4]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == []

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == []


def test_continuation_with_delayed_backup(tmp_path):
    init_db_dir(tmp_path, 1)
    print(tmp_path)
    recovery_config = RecoveryConfig(str(tmp_path), backup_interval=FIVE_TD * 2)

    inp = [
        0,
        TestingSource.EOF(),
        1,
        TestingSource.EOF(),
        2,
        TestingSource.EOF(),
        3,
        TestingSource.EOF(),
        4,
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting the epoch interval to 5 sec means we should only
    # snapshot on EOF. But we have set the backup interval to
    # effectively -2 epochs so we should get delayed backup after a
    # few executions.
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [1]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [2]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [3]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [4]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == []


def test_snapshot_interval_default_unchanged(tmp_path):
    init_db_dir(tmp_path, 1)
    # An explicit `None` must be the same as not setting it: snapshot
    # every epoch.
    recovery_config = RecoveryConfig(str(tmp_path), snapshot_interval=None)

    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]


def _slow_running_sum(state, item):
    # Each item takes longer than the epoch interval, so every item
    # lands in its own epoch. That makes the epoch arithmetic in the
    # test below deterministic.
    time.sleep(0.08)
    total = (state or 0) + item
    return (total, total)


def _totals(keyed_outputs):
    return [total for _key, total in keyed_outputs]


@mark.parametrize(
    ("abort_at", "resume_at"),
    [
        # The abort after 7 items leaves the epoch-6 snapshot as the
        # last one, so item 6 is replayed.
        (7, 6),
        # The abort after 8 items leaves the epoch-9 snapshot intact,
        # so nothing is replayed.
        (8, 8),
    ],
)
def test_snapshot_interval_resumes_from_boundary(tmp_path, abort_at, resume_at):
    init_db_dir(tmp_path, 1)
    # 0.15s snapshot over 0.05s epochs is 3 epochs per snapshot.
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=timedelta(seconds=0.15)
    )

    items = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    # Powers of two make every prefix sum unique, so a resumed running
    # sum identifies exactly which prefix the snapshot captured.
    prefix_sums = list(accumulate(items))
    # `stateful_map` needs `(key, value)` pairs; use a single key.
    # Item i is emitted in epoch i + 1, and each item is slow enough to
    # close its own epoch. With 3 epochs per snapshot the boundaries are
    # at epochs 3, 6, and 9. The abort after `abort_at` items leaves the
    # last boundary at or before epoch `abort_at + 1` as the resume
    # point, so resume starts at item `resume_at`.
    inp = (
        [("k", x) for x in items[:abort_at]]
        + [TestingSource.ABORT()]
        + [("k", x) for x in items[abort_at:]]
    )
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("sum", s, _slow_running_sum)
    op.output("out", s, TestingSink(out))

    run_main(
        flow, epoch_interval=timedelta(seconds=0.05), recovery_config=recovery_config
    )
    assert _totals(out) == prefix_sums[:abort_at]
    first_run = _totals(out)

    # Resume must start from the last boundary snapshot, not from the
    # abort point and not from the beginning of input.
    out.clear()
    run_main(
        flow, epoch_interval=timedelta(seconds=0.05), recovery_config=recovery_config
    )
    resumed = _totals(out)
    # The resumed state is the snapshot at the last surviving boundary,
    # so the replay starts at `resume_at`.
    assert resumed == prefix_sums[resume_at:]

    # At-least-once: the union of both executions is every item.
    assert set(first_run) | set(resumed) == set(prefix_sums)

    out.clear()
    run_main(
        flow, epoch_interval=timedelta(seconds=0.05), recovery_config=recovery_config
    )
    assert out == []


def test_snapshot_interval_eof_snapshots(tmp_path):
    init_db_dir(tmp_path, 1)
    # The snapshot interval is far longer than the whole run, so only
    # EOF can take a snapshot. Resume must still work.
    recovery_config = RecoveryConfig(
        str(tmp_path), snapshot_interval=timedelta(seconds=3600)
    )

    inp = [0, 1, 2, TestingSource.EOF(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [3, 4]

    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == []


def keep_max(max_val, new_val):
    if max_val is None:
        max_val = 0
    max_val = max(max_val, new_val)
    return (max_val, max_val)


def build_keep_max_dataflow(inp, out):
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_map("max", s, keep_max)
    op.output("out", s, TestingSink(out))
    return flow


def test_rescale(tmp_path):
    init_db_dir(tmp_path, 3)
    recovery_config = RecoveryConfig(str(tmp_path))

    inp = [
        ("a", 4),
        ("b", 4),
        TestingSource.EOF(),
        ("a", 1),
        ("b", 5),
        TestingSource.EOF(),
        ("a", 8),
        ("b", 1),
    ]
    out = []

    flow = build_keep_max_dataflow(inp, out)

    def entry_point(worker_count_per_proc):
        cluster_main(
            flow,
            addresses=[],
            proc_id=0,
            epoch_interval=ZERO_TD,
            recovery_config=recovery_config,
            worker_count_per_proc=worker_count_per_proc,
        )

    # We're going to do 2 continuations with different numbers of
    # workers each time. Start with 3 workers.
    entry_point(3)
    assert out == [
        ("a", 4),
        ("b", 4),
    ]

    # Continue with 5 workers.
    out.clear()
    entry_point(5)
    assert out == [
        ("a", 4),
        ("b", 5),
    ]

    # Continue again resizing down to 1 worker.
    out.clear()
    entry_point(1)
    assert out == [
        ("a", 8),
        ("b", 5),
    ]


def test_no_parts(tmp_path):
    # Don't init_db_dir.
    recovery_config = RecoveryConfig(str(tmp_path))

    inp = []
    out = []

    flow = build_keep_max_dataflow(inp, out)

    with raises(NoPartitionsError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)


def test_missing_parts(tmp_path):
    init_db_dir(tmp_path, 3)
    recovery_config = RecoveryConfig(str(tmp_path))

    os.remove(tmp_path / "part-0.sqlite3")

    inp = []
    out = []

    flow = build_keep_max_dataflow(inp, out)

    with raises(MissingPartitionsError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)


def test_inconsistent_parts(tmp_path):
    part_count = 3

    init_db_dir(tmp_path, part_count)
    recovery_config = RecoveryConfig(str(tmp_path), backup_interval=ZERO_TD)

    # Take an snapshot of all the initial partitions. Snapshot
    # everything just to help with debugging this test.
    for i in range(part_count):
        shutil.copy(tmp_path / f"part-{i}.sqlite3", tmp_path / f"part-{i}.run0")

    inp = [
        ("a", 4),
        ("b", 4),
        TestingSource.ABORT(),
        ("a", 1),
        ("b", 5),
    ]
    out = []

    flow = build_keep_max_dataflow(inp, out)

    # Run the dataflow initially to completion.
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", 4),
        ("b", 4),
    ]

    # Take an snapshot of all the partitions after the first run.
    for i in range(part_count):
        shutil.copy(tmp_path / f"part-{i}.sqlite3", tmp_path / f"part-{i}.run1")

    # Continue but overwrite partition 0 with initial version. Because
    # the backup interval is 0, we should have already thrown away
    # state to resume at the initial epoch 1.
    out.clear()
    shutil.copy(tmp_path / "part-0.run0", tmp_path / "part-0.sqlite3")
    with raises(InconsistentPartitionsError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)


def _slow_identity(item):
    # Each item takes longer than the epoch interval, so every item
    # closes its own epoch.
    time.sleep(0.08)
    return item


@requires_strict_tables
def test_backup_within_interval_resumes_with_snapshot_interval(tmp_path):
    # The backup interval is one epoch and each snapshot covers three
    # epochs. A backup of one partition, taken one item before the live
    # store moved on, must still resume with no loss.
    epoch = timedelta(seconds=0.05)
    store = tmp_path / "store"
    store.mkdir()
    init_db_dir(store, 3)
    recovery_config = RecoveryConfig(
        str(store), backup_interval=epoch, snapshot_interval=epoch * 3
    )
    items = list(range(12))

    def build(source, out):
        df = Dataflow("backup_within")
        s = op.input("inp", df, source)
        s = op.map("slow", s, _slow_identity)
        op.output("out", s, TestingSink(out))
        return df

    first = []
    run_main(
        build(TestingSource(items[:4] + [TestingSource.ABORT()] + items[4:]), first),
        epoch_interval=epoch,
        recovery_config=recovery_config,
    )
    backup = tmp_path / "backup"
    shutil.copytree(store, backup)

    live = []
    run_main(
        build(TestingSource(items[:5] + [TestingSource.ABORT()] + items[5:]), live),
        epoch_interval=epoch,
        recovery_config=recovery_config,
    )

    # Restore the partition that holds the newest progress rows from
    # the backup; the others stay as the live store left them.
    for part in sorted(store.glob("part-*.sqlite3")):
        with closing(sqlite3.connect(part)) as conn:
            rows = conn.execute("SELECT COUNT(*) FROM fronts").fetchone()[0]
        if rows:
            shutil.copy(backup / part.name, part)
            break

    resumed = []
    run_main(
        build(TestingSource(items), resumed),
        epoch_interval=epoch,
        recovery_config=recovery_config,
    )

    assert set(first + live + resumed) == set(items)


NUM_BATCHES = 50
BATCH_SIZE = 10


class _NumberedBatchPartition(StatelessSourcePartition[int]):
    """Emit one batch of consecutive numbers per `next_batch` call."""

    def __init__(self, batch_ids):
        self._batch_ids = list(batch_ids)
        self._next = 0

    def next_batch(self):
        if self._next >= len(self._batch_ids):
            raise StopIteration()
        batch_id = self._batch_ids[self._next]
        self._next += 1
        start = batch_id * BATCH_SIZE
        return list(range(start, start + BATCH_SIZE))


class _NumberedBatchSource(DynamicSource[int]):
    def build(self, _step_id, worker_index, worker_count):
        # Each worker reads a disjoint set of batches.
        return _NumberedBatchPartition(range(worker_index, NUM_BATCHES, worker_count))


def test_recovery_stateless_flow_reads_all_input(recovery_config):
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, _NumberedBatchSource())
    s = op.map("id", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=TEN_TD, recovery_config=recovery_config)
    assert sorted(out) == list(range(NUM_BATCHES * BATCH_SIZE))


def test_recovery_stateless_flow_reads_all_input_cluster(recovery_config):
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, _NumberedBatchSource())
    s = op.map("id", s, lambda x: x)
    op.output("out", s, TestingSink(out))

    cluster_main(
        flow,
        [],
        0,
        epoch_interval=TEN_TD,
        recovery_config=recovery_config,
        worker_count_per_proc=2,
    )
    assert sorted(out) == list(range(NUM_BATCHES * BATCH_SIZE))


_READ_SNAP_EPOCHS = """
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
rows = conn.execute("SELECT DISTINCT snap_epoch FROM snaps").fetchall()
print(" ".join(str(epoch) for (epoch,) in rows))
"""


def _snap_epochs_after_run(db_dir):
    """Distinct snapshot epochs in recovery partition 0.

    Read in a child process, so the store is never opened by the test
    process while or after an execution uses it.
    """
    proc = subprocess.run(
        [sys.executable, "-c", _READ_SNAP_EPOCHS, str(db_dir / "part-0.sqlite3")],
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted(int(epoch) for epoch in proc.stdout.split())


def _sleep_then_sum(state, item):
    # Each item lasts several 500 us epochs.
    time.sleep(0.001)
    total = (state or 0) + item
    return (total, total)


@requires_strict_tables
def test_sub_millisecond_epoch_interval_garbage_collects_snapshots(tmp_path):
    init_db_dir(tmp_path, 1)
    # 5 ms over 500 us epochs is a commit delay of 10 epochs. Dividing
    # whole milliseconds made that delay infinite, so no commit ever
    # ran and every old snapshot was kept.
    recovery_config = RecoveryConfig(
        str(tmp_path), backup_interval=timedelta(milliseconds=5)
    )
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([("k", x) for x in range(200)]))
    s = op.stateful_map("sum", s, _sleep_then_sum)
    op.output("out", s, TestingSink(out))

    run_main(
        flow,
        epoch_interval=timedelta(microseconds=500),
        recovery_config=recovery_config,
    )
    assert len(out) == 200

    # The run spans about 200 epochs, so without garbage collection
    # the store would keep a snapshot for nearly all of them.
    epochs = _snap_epochs_after_run(tmp_path)
    assert max(epochs) >= 100, epochs
    assert len(epochs) <= 30, epochs
