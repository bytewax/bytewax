from datetime import datetime, timedelta
from typing import Any, Iterable, Optional, Tuple

from bytewax.dataflow import Dataflow
from bytewax.operators import UnaryLogic
from bytewax.testing import TestingSink, TestingSource, run_main

ZERO_TD = timedelta(seconds=0)


def test_unary_on_item_discard():
    inp = [1, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, _now: datetime, state: Any):
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "ITEM"
            return ([(old_state, self._state)], UnaryLogic.DISCARD)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def notify_at(self) -> Optional[datetime]:
            return None

        def snapshot(self) -> Any:
            return self._state

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
    inp = [1, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, _now: datetime, state: Any):
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "ITEM"
            return ([(old_state, self._state)], UnaryLogic.RETAIN)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def notify_at(self) -> Optional[datetime]:
            return None

        def snapshot(self) -> Any:
            return self._state

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
    inp = [1, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, now: datetime, state: Any):
            self._notify = now
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "NOTIFY"
            return ([(old_state, self._state)], UnaryLogic.DISCARD)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def notify_at(self) -> Optional[datetime]:
            return self._notify

        def snapshot(self) -> Any:
            return self._state

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
    inp = [1, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, now: datetime, state: Any):
            self._notify: Optional[datetime] = now
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            self._notify = now
            return ([], UnaryLogic.RETAIN)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "NOTIFY"
            self._notify = None
            return ([(old_state, self._state)], UnaryLogic.RETAIN)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def notify_at(self) -> Optional[datetime]:
            return self._notify

        def snapshot(self) -> Any:
            return self._state

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
    inp = [1, TestingSource.EOF, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, _now: datetime, state: Any):
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "EOF"
            return ([(old_state, self._state)], UnaryLogic.DISCARD)

        def notify_at(self) -> Optional[datetime]:
            return None

        def snapshot(self) -> Any:
            return self._state

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
    inp = [1, TestingSource.EOF, 2]
    out = []

    class TestLogic(UnaryLogic):
        def __init__(self, _now: datetime, state: Any):
            self._state = state if state is not None else "NEW"

        def on_item(self, now: datetime, value: Any) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_notify(self, sched: datetime) -> Tuple[Iterable[Any], bool]:
            return ([], UnaryLogic.RETAIN)

        def on_eof(self) -> Tuple[Iterable[Any], bool]:
            old_state = self._state
            self._state = "EOF"
            return ([(old_state, self._state)], UnaryLogic.RETAIN)

        def notify_at(self) -> Optional[datetime]:
            return None

        def snapshot(self) -> Any:
            return self._state

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
