from datetime import timedelta
from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulBatchLogic
from bytewax.testing import TestingSink, TestingSource, run_main
from typing_extensions import override

ZERO_TD = timedelta(seconds=0)

SNAPSHOT_ORDER: List[str] = []


class TagLogic(StatefulBatchLogic[str, str, str]):
    """Tags each key by the first value it sees.

    That makes the order the runtime visits keys in visible: through
    the output order, and through the order of `snapshot` calls.

    """

    def __init__(self, resume_state: Optional[str]) -> None:
        self._tag = resume_state

    @override
    def on_batch(self, values: List[str]) -> Tuple[List[str], bool]:
        if self._tag is None:
            self._tag = values[0]
        return ([f"{v}!" for v in values], StatefulBatchLogic.RETAIN)

    @override
    def on_notify(self) -> Tuple[List[str], bool]:
        return ([], StatefulBatchLogic.RETAIN)

    @override
    def on_eof(self) -> Tuple[List[str], bool]:
        return ([f"eof:{self._tag}"], StatefulBatchLogic.RETAIN)

    @override
    def notify_at(self) -> None:
        return None

    @override
    def snapshot(self) -> str:
        assert self._tag is not None
        SNAPSHOT_ORDER.append(self._tag)
        return self._tag


INP = [
    ("c", "c1"),
    ("b", "b1"),
    ("d", "d1"),
    ("a", "a1"),
    ("b", "b2"),
    ("a", "a2"),
    ("e", "e1"),
    ("c", "c2"),
]


def test_stateful_batch_visits_keys_in_key_order():
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP, batch_size=len(INP)))
    s = op.stateful_batch("stateful_batch", s, TagLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    # One batch: keys are visited in key order, and each key's values
    # keep the order they arrived in.
    assert out == [
        ("a", "a1!"),
        ("a", "a2!"),
        ("b", "b1!"),
        ("b", "b2!"),
        ("c", "c1!"),
        ("c", "c2!"),
        ("d", "d1!"),
        ("e", "e1!"),
        # And at EOF.
        ("a", "eof:a1"),
        ("b", "eof:b1"),
        ("c", "eof:c1"),
        ("d", "eof:d1"),
        ("e", "eof:e1"),
    ]


def test_stateful_batch_snapshots_keys_in_key_order(recovery_config):
    out = []
    SNAPSHOT_ORDER.clear()

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(INP, batch_size=len(INP)))
    s = op.stateful_batch("stateful_batch", s, TagLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    # Every key is snapshotted when the batch's epoch ends and again
    # at EOF, and each time they go in key order.
    assert len(SNAPSHOT_ORDER) > 0
    assert len(SNAPSHOT_ORDER) % 5 == 0
    for i in range(0, len(SNAPSHOT_ORDER), 5):
        assert SNAPSHOT_ORDER[i : i + 5] == ["a1", "b1", "c1", "d1", "e1"]
