from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSink, TestingSource, cluster_main, run_main

ZERO_TD = timedelta(seconds=0)
FIVE_TD = timedelta(seconds=5)


def test_abort_no_snapshots(tmp_path):
    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting batch_backup to True and epoch interval to 5 sec
    # means we shouldn't have a snapshot when the abort happens.
    recovery_config = RecoveryConfig(str(tmp_path), batch_backup=True)
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    # So resume should re-play all input.
    out.clear()
    run_main(flow, epoch_interval=FIVE_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2, 3, 4]


def test_abort_with_snapshots(tmp_path):
    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting batch_backup to False means we will have a snapshot after each item.
    recovery_config = RecoveryConfig(str(tmp_path), batch_backup=False)
    run_main(flow, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    # We should resume as if it was an EOF.
    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]


def test_continuation(tmp_path):
    inp = [0, 1, 2, TestingSource.EOF(), 3, 4]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, TestingSink(out))

    # Setting batch_backup to True and epoch interval to 5 sec means we should only
    # snapshot on EOF.
    recovery_config = RecoveryConfig(str(tmp_path), batch_backup=True)
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


def test_stateful_batch_recovery(tmp_path):
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
    run_main(flow, recovery_config=recovery_config)
    assert out == [
        ("a", 4),
        ("b", 4),
    ]

    out.clear()
    run_main(flow, recovery_config=recovery_config)
    assert out == [
        ("a", 4),
        ("b", 5),
    ]

    out.clear()
    run_main(flow, recovery_config=recovery_config)
    assert out == [
        ("a", 8),
        ("b", 5),
    ]


def test_rescale(tmp_path):
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
