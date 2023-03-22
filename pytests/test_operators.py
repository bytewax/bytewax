import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from threading import Event

from pytest import raises

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput, TestingOutput
from bytewax.window import EventClockConfig, TumblingWindow

ZERO_TD = timedelta(seconds=0)


def test_requires_input():
    flow = Dataflow()
    out = []
    flow.output("out", TestingOutput(out))

    with raises(ValueError):
        run_main(flow)


def test_requires_output():
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))

    with raises(ValueError):
        run_main(flow)


def test_map():
    flow = Dataflow()

    inp = [0, 1, 2]
    flow.input("inp", TestingInput(inp))

    def add_one(item):
        return item + 1

    flow.map(add_one)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_filter_map():
    flow = Dataflow()

    inp = [0, 1, 2, 3, 4, 5]
    flow.input("inp", TestingInput(inp))

    def make_odd(item):
        if item % 2 != 0:
            return None
        return item + 1

    flow.filter_map(make_odd)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 3, 5])


def test_flat_map():
    flow = Dataflow()

    inp = ["split this"]
    flow.input("inp", TestingInput(inp))

    def split_into_words(sentence):
        return sentence.split()

    flow.flat_map(split_into_words)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted(["split", "this"])


def test_filter():
    flow = Dataflow()

    inp = [1, 2, 3]
    flow.input("inp", TestingInput(inp))

    def is_odd(item):
        return item % 2 != 0

    flow.filter(is_odd)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 3])


def test_inspect():
    flow = Dataflow()

    inp = ["a"]
    flow.input("inp", TestingInput(inp))

    seen = []
    flow.inspect(seen.append)

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted(["a"])


def test_inspect_epoch():
    flow = Dataflow()

    inp = ["a"]
    flow.input("inp", TestingInput(inp))

    seen = []
    flow.inspect_epoch(lambda epoch, item: seen.append((epoch, item)))

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted([(0, "a")])


def test_reduce(recovery_config):
    flow = Dataflow()

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        "BOOM",
        {"user": "b", "type": "login"},
        {"user": "a", "type": "logout"},
        {"user": "b", "type": "logout"},
    ]
    flow.input("inp", TestingInput(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                raise RuntimeError("BOOM")
            else:
                return []
        else:
            return [item]

    flow.flat_map(trigger)

    def user_as_key(event):
        return (event["user"], [event])

    flow.map(user_as_key)

    def extend_session(session, event):
        return session + event

    def session_complete(session):
        return any(event["type"] == "logout" for event in session)

    flow.reduce("sessionizer", extend_session, session_complete)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert sorted(out) == sorted([])

    # Disable bomb.
    armed.clear()
    out.clear()

    # Recover
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            (
                "a",
                [
                    {"user": "a", "type": "login"},
                    {"user": "a", "type": "post"},
                    {"user": "a", "type": "logout"},
                ],
            ),
            (
                "b",
                [
                    {"user": "b", "type": "login"},
                    {"user": "b", "type": "logout"},
                ],
            ),
        ]
    )


def test_stateful_map(recovery_config):
    flow = Dataflow()

    inp = [
        "a",
        "b",
        "BOOM",
        "b",
        "c",
        "a",
    ]
    flow.input("inp", TestingInput(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                raise RuntimeError("BOOM")
            else:
                return []
        else:
            return [item]

    flow.flat_map(trigger)

    def add_key(item):
        return item, item

    flow.map(add_key)

    def build_seen():
        return set()

    def check(seen, value):
        if value in seen:
            return seen, True
        else:
            seen.add(value)
            return seen, False

    flow.stateful_map("build_seen", build_seen, check)

    def remove_seen(key__is_seen):
        key, is_seen = key__is_seen
        if not is_seen:
            return [key]
        else:
            return []

    flow.flat_map(remove_seen)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            "a",
            "b",
        ]
    )

    # Disable bomb
    armed.clear()
    out.clear()

    # Recover
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert sorted(out) == sorted(
        [
            "c",
        ]
    )


def test_stateful_map_error_on_non_kv_tuple():
    flow = Dataflow()

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"},
    ]
    flow.input("inp", TestingInput(inp))

    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    flow.stateful_map("running_count", lambda: defaultdict(int), running_count)

    out = []
    flow.output("out", TestingOutput(out))

    expect = (
        "Dataflow requires a `(key, value)` 2-tuple as input to every stateful "
        "operator for routing; got `{'user': 'a', 'type': 'login'}` instead"
    )

    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)


def test_stateful_map_error_on_non_string_key():
    flow = Dataflow()

    # Note that the resulting key will be an int.
    inp = [
        {"user": {"id": 1}, "type": "login"},
        {"user": {"id": 1}, "type": "post"},
        {"user": {"id": 2}, "type": "login"},
        {"user": {"id": 2}, "type": "post"},
    ]
    flow.input("inp", TestingInput(inp))

    def add_key(event):
        # Note that event["user"] is an entire dict, but keys must be
        # strings.
        return event["user"], event

    flow.map(add_key)

    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    flow.stateful_map("running_count", lambda: defaultdict(int), running_count)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(
        TypeError,
        match=re.escape(
            "Stateful logic functions must return string or integer keys in "
            "`(key, value)`; got `{'id': 1}` instead"
        ),
    ):
        run_main(flow)


def test_reduce_window(recovery_config):
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    flow = Dataflow()

    inp = [
        ("ALL", {"time": align_to, "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": 1}),
        "BOOM",
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": 1}),
    ]

    flow.input("inp", TestingInput(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                raise RuntimeError("BOOM")
            else:
                return []
        else:
            return [item]

    flow.flat_map(trigger)

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def add(acc, x):
        acc["val"] += x["val"]
        return acc

    flow.reduce_window("add", clock_config, window_config, add)

    def extract_val(key__event):
        key, event = key__event
        return (key, event["val"])

    flow.map(extract_val)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Only the first window closed here
    assert sorted(out) == sorted([("ALL", 3)])

    # Disable bomb
    armed.clear()
    out.clear()

    # Recover
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # But it remembers the first item of the second window.
    assert sorted(out) == sorted([("ALL", 2)])


def test_fold_window(recovery_config):
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    flow = Dataflow()

    inp = [
        {"time": align_to, "user": "a", "type": "login"},
        {"time": align_to + timedelta(seconds=4), "user": "a", "type": "post"},
        {"time": align_to + timedelta(seconds=8), "user": "a", "type": "post"},
        # First 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=12), "user": "b", "type": "login"},
        {"time": align_to + timedelta(seconds=16), "user": "a", "type": "post"},
        # Crash before closing the window.
        # It will be emitted during the second run.
        "BOOM",
        # Second 10 sec window closes during processing this input.
        {"time": align_to + timedelta(seconds=20), "user": "b", "type": "post"},
        {"time": align_to + timedelta(seconds=24), "user": "b", "type": "post"},
    ]

    flow.input("inp", TestingInput(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                raise RuntimeError("BOOM")
            else:
                return []
        else:
            return [item]

    flow.flat_map(trigger)

    def key_off_user(event):
        return (event["user"], event)

    flow.map(key_off_user)

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def count(counts, event):
        typ = event["type"]
        if typ not in counts:
            counts[typ] = 0
        counts[typ] += 1
        return counts

    flow.fold_window("count", clock_config, window_config, dict, count)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert out == [
        ("a", {"login": 1, "post": 2}),
    ]

    # Disable bomb
    armed.clear()
    out.clear()

    # Recover
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    assert len(out) == 3
    assert ("b", {"login": 1}) in out
    assert ("b", {"post": 2}) in out
    assert ("a", {"post": 1}) in out


def test_collect_window(recovery_config):
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    flow = Dataflow()

    inp = [
        ("ALL", {"time": align_to, "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": 1}),
        "BOOM",
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": 1}),
    ]

    flow.input("inp", TestingInput(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                raise RuntimeError("BOOM")
            else:
                return []
        else:
            return [item]

    flow.flat_map(trigger)

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    flow.collect_window("add", clock_config, window_config)

    out = []
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # Only the first window closed here
    assert out == [
        (
            "ALL",
            [
                {"time": align_to, "val": 1},
                {"time": align_to + timedelta(seconds=4), "val": 1},
                {"time": align_to + timedelta(seconds=8), "val": 1},
            ],
        )
    ]

    # Disable bomb
    armed.clear()
    out.clear()

    # Recover
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)

    # But it remembers the first item of the second window.
    assert out == [
        (
            "ALL",
            [
                {"time": align_to + timedelta(seconds=12), "val": 1},
                {"time": align_to + timedelta(seconds=13), "val": 1},
            ],
        )
    ]


def test_output_emits_downstream():
    flow = Dataflow()

    inp = [0, 1, 2]
    flow.input("inp", TestingInput(inp))

    out1 = []
    flow.output("out1", TestingOutput(out1))

    out2 = []
    flow.output("out2", TestingOutput(out2))

    run_main(flow)

    assert sorted(out1) == sorted(inp)
    assert sorted(out2) == sorted(inp)
