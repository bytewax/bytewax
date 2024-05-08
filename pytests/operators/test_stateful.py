from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic
from bytewax.recovery import RecoveryConfig
from bytewax.testing import TestingSink, TestingSource, run_main
from typing_extensions import override

ZERO_TD = timedelta(seconds=0)


class BaseTestLogic(StatefulLogic):
    """Testing logic.

    Every time there is an event, emit the state transition. Then use
    the class settings to decide wheither to throw away the state.

    Notification will happen after each item immediately and will be
    cleared when `on_notify` is run.

    """

    item_triggers_notify = False
    after_item = StatefulLogic.RETAIN
    after_notify = StatefulLogic.RETAIN
    after_eof = StatefulLogic.RETAIN

    def __init__(self, state: Any):
        self._notify_at: Optional[datetime] = None
        self._state = state if state is not None else "NEW"

    @override
    def on_item(self, value: Any) -> Tuple[List[Any], bool]:
        if self.item_triggers_notify:
            self._notify_at = datetime.now(timezone.utc)

        old_state = self._state
        self._state = "ITEM"
        return ([(old_state, self._state)], self.after_item)

    @override
    def on_notify(self) -> Tuple[List[Any], bool]:
        self._notify_at = None

        old_state = self._state
        self._state = "NOTIFY"
        return ([(old_state, self._state)], self.after_notify)

    @override
    def on_eof(self) -> Tuple[List[Any], bool]:
        old_state = self._state
        self._state = "EOF"
        return ([(old_state, self._state)], self.after_eof)

    @override
    def notify_at(self) -> Optional[datetime]:
        return self._notify_at

    @override
    def snapshot(self) -> Any:
        return self._state


def test_stateful_on_item_discard():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_item = StatefulLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("NEW", "ITEM")),
    ]


def test_stateful_on_item_retain():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_item = StatefulLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "ITEM")),
    ]


def test_stateful_on_notify_discard():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        item_triggers_notify = True
        after_notify = StatefulLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
    ]


def test_stateful_on_notify_retain():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        item_triggers_notify = True
        after_notify = StatefulLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
        ("ALL", ("NOTIFY", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
    ]


def test_stateful_on_eof_discard(recovery_config):
    inp = [1, TestingSource.EOF(), 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = StatefulLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM")), ("ALL", ("ITEM", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM"))]


def test_stateful_on_eof_retain(recovery_config):
    inp = [1, TestingSource.EOF(), 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = StatefulLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.stateful("stateful", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM")), ("ALL", ("ITEM", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("EOF", "ITEM"))]


class KeepLastLogic(StatefulLogic):
    def __init__(self, resume_state: Any):
        self._state = resume_state

    @override
    def on_item(self, value: Any) -> Tuple[List[Any], bool]:
        old_state = self._state
        self._state = value
        return ([(old_state, self._state)], self._state == "DISCARD")

    @override
    def on_notify(self) -> Tuple[List[Any], bool]:
        return ([], StatefulLogic.RETAIN)

    @override
    def on_eof(self) -> Tuple[List[Any], bool]:
        return ([], StatefulLogic.RETAIN)

    @override
    def notify_at(self) -> Optional[datetime]:
        return None

    @override
    def snapshot(self) -> Any:
        return self._state


def test_stateful_keeps_logic_per_key():
    inp = [("a", "a1"), ("b", "b1"), ("a", "a2"), ("b", "b2")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful("stateful", s, KeepLastLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("a", (None, "a1")),
        ("b", (None, "b1")),
        ("a", ("a1", "a2")),
        ("b", ("b1", "b2")),
    ]


def test_stateful_snapshots_logic_per_key(recovery_config):
    inp = [("a", "a1"), ("b", "b1"), TestingSource.ABORT(), ("a", "a2"), ("b", "b2")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful("stateful", s, KeepLastLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", (None, "a1")),
        ("b", (None, "b1")),
    ]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", ("a1", "a2")),
        ("b", ("b1", "b2")),
    ]


def test_stateful_snapshots_discard_per_key(recovery_config):
    inp = [
        ("a", "a1"),
        ("b", "b1"),
        ("a", "DISCARD"),
        ("b", "b2"),
        TestingSource.ABORT(),
        ("a", "a3"),
        ("b", "b3"),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful("stateful", s, KeepLastLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", (None, "a1")),
        ("b", (None, "b1")),
        ("a", ("a1", "DISCARD")),
        ("b", ("b1", "b2")),
    ]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", (None, "a3")),
        ("b", ("b2", "b3")),
    ]


def test_stateful_recovers_older_snapshots(recovery_config):
    inp = [
        ("a", "a1"),
        TestingSource.ABORT(),
        ("b", "b1"),
        TestingSource.ABORT(),
        ("c", "c1"),
        ("b", "DISCARD"),
        TestingSource.ABORT(),
        ("a", "a2"),
        ("b", "b2"),
        ("c", "c2"),
    ]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful("stateful", s, KeepLastLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("a", (None, "a1")),
    ]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("b", (None, "b1")),
    ]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("b", ("b1", "b1")),
        ("c", (None, "c1")),
        ("b", ("b1", "DISCARD")),
    ]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [
        ("b", (None, "DISCARD")),
        ("a", ("a1", "a2")),
        ("b", (None, "b2")),
        ("c", ("c1", "c2")),
    ]
