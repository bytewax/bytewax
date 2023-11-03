import os
import shutil
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.recovery import (
    InconsistentPartitionsError,
    MissingPartitionsError,
    NoPartitionsError,
    RecoveryConfig,
    init_db_dir,
)
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import raises

ZERO_TD = timedelta(seconds=0)


def test_continuation(recovery_config):
    inp = [0, 1, 2, TestingSource.EOF(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s.output("out", TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == []


def keep_max(max_val, new_val):
    max_val = max(max_val, new_val)
    return (max_val, max_val)


def build_keep_max_dataflow(inp, out):
    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_assert("keyed")
    s = s.stateful_map("max", lambda: 0, keep_max)
    s.output("out", TestingSink(out))
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
