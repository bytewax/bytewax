from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import UnaryLogic
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)


class BaseTestLogic(UnaryLogic):
    """Testing logic.

    Every time there is an event, emit the state transition. Then use
    the class settings to decide wheither to throw away the state.

    Notification will happen after each item immediately and will be
    cleared when `on_notify` is run.

    """

    item_triggers_notify = False
    after_item = UnaryLogic.RETAIN
    after_notify = UnaryLogic.RETAIN
    after_eof = UnaryLogic.RETAIN

    def __init__(self, _now: datetime, state: Any):
        self._notify_at: Optional[datetime] = None
        self._state = state if state is not None else "NEW"

    def on_item(self, now: datetime, value: Any) -> Tuple[List[Any], bool]:
        if self.item_triggers_notify:
            self._notify_at = now

        old_state = self._state
        self._state = "ITEM"
        return ([(old_state, self._state)], self.after_item)

    def on_notify(self, sched: datetime) -> Tuple[List[Any], bool]:
        self._notify_at = None

        old_state = self._state
        self._state = "NOTIFY"
        return ([(old_state, self._state)], self.after_notify)

    def on_eof(self) -> Tuple[List[Any], bool]:
        old_state = self._state
        self._state = "EOF"
        return ([(old_state, self._state)], self.after_eof)

    def notify_at(self) -> Optional[datetime]:
        return self._notify_at

    def snapshot(self) -> Any:
        return self._state


def test_unary_on_item_discard():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_item = UnaryLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("NEW", "ITEM")),
    ]


def test_unary_on_item_retain():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_item = UnaryLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "ITEM")),
    ]


def test_unary_on_notify_discard():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        item_triggers_notify = True
        after_notify = UnaryLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
    ]


def test_unary_on_notify_retain():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        item_triggers_notify = True
        after_notify = UnaryLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("ALL", ("NEW", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
        ("ALL", ("NOTIFY", "ITEM")),
        ("ALL", ("ITEM", "NOTIFY")),
    ]


def test_unary_on_eof_discard(recovery_config):
    inp = [1, TestingSource.EOF(), 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = UnaryLogic.DISCARD

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM")), ("ALL", ("ITEM", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM"))]


def test_unary_on_eof_retain(recovery_config):
    inp = [1, TestingSource.EOF(), 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = UnaryLogic.RETAIN

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.unary("unary", s, TestLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "ITEM")), ("ALL", ("ITEM", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("EOF", "ITEM"))]


class KeepLastLogic(UnaryLogic):
    def __init__(self, _now: datetime, resume_state: Any):
        self._state = resume_state

    def on_item(self, _now: datetime, value: Any) -> Tuple[List[Any], bool]:
        old_state = self._state
        self._state = value
        return ([(old_state, self._state)], self._state == "DISCARD")

    def on_notify(self, sched: datetime) -> Tuple[List[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def on_eof(self) -> Tuple[List[Any], bool]:
        return ([], UnaryLogic.RETAIN)

    def notify_at(self) -> Optional[datetime]:
        return None

    def snapshot(self) -> Any:
        return self._state


def test_unary_keeps_logic_per_key():
    inp = [("a", "a1"), ("b", "b1"), ("a", "a2"), ("b", "b2")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.unary("unary", s, KeepLastLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD)
    assert out == [
        ("a", (None, "a1")),
        ("b", (None, "b1")),
        ("a", ("a1", "a2")),
        ("b", ("b1", "b2")),
    ]


def test_unary_snapshots_logic_per_key(recovery_config):
    inp = [("a", "a1"), ("b", "b1"), TestingSource.ABORT(), ("a", "a2"), ("b", "b2")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.unary("unary", s, KeepLastLogic)
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


def test_unary_snapshots_discard_per_key(recovery_config):
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
    s = op.unary("unary", s, KeepLastLogic)
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
