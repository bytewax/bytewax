from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.operators import UnaryLogic
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)


class BaseTestLogic(UnaryLogic):
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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow)
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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow)
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
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", ("NEW", "NOTIFY")),
        ("ALL", ("NEW", "NOTIFY")),
    ]


def test_unary_on_notify_retain():
    inp = [1, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        item_triggers_notify = True
        after_notify = UnaryLogic.RETAIN

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [
        ("ALL", ("NEW", "NOTIFY")),
        ("ALL", ("NOTIFY", "NOTIFY")),
    ]


def test_unary_on_eof_discard(recovery_config):
    inp = [1, TestingSource.EOF, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = UnaryLogic.DISCARD

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "EOF"))]


def test_unary_on_eof_retain(recovery_config):
    inp = [1, TestingSource.EOF, 2, TestingSource.ABORT()]
    out = []

    class TestLogic(BaseTestLogic):
        after_eof = UnaryLogic.RETAIN

    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(inp))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.unary("unary", TestLogic)
    s.output("out", TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("NEW", "EOF"))]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [("ALL", ("EOF", "EOF"))]
    assert out == [("ALL", ("EOF", "EOF"))]
