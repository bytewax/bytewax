import json
import os
import shutil
from datetime import timedelta
from typing import Any, override

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.recovery import (
    InconsistentPartitionsError,
    MissingPartitionsError,
    NoPartitionsError,
    RecoveryConfig,
    init_db_dir,
)
from bytewax.serde import Serde, set_serde_obj
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main
from pytest import raises

ZERO_TD = timedelta(seconds=0)
FIVE_TD = timedelta(seconds=5)


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


def test_custom_serde(tmp_path):
    class JSONSerde(Serde):
        @override
        def ser(self, obj: Any) -> bytes:
            return json.dumps(obj).encode("utf-8")

        @override
        def de(self, s: bytes) -> Any:
            print(s)
            return json.loads(s)

    init_db_dir(tmp_path, 1)
    set_serde_obj(JSONSerde())
    recovery_config = RecoveryConfig(str(tmp_path), backup_interval=ZERO_TD)
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
