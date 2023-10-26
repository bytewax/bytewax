import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from threading import Event

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from bytewax.window import EventClockConfig, TumblingWindow
from pytest import raises

ZERO_TD = timedelta(seconds=0)


def test_batch():
    in_data = [("ALL", x) for x in range(10)]
    flow = Dataflow("test_df")
    stream = flow.input("in", TestingSource(in_data)).assert_keyed("key")
    # Use a long timeout to avoid triggering that.
    # We can't easily test system time based behavior.
    stream = stream.batch("batch", timedelta(seconds=10), 3)
    out = []
    stream.output("out", TestingSink(out))
    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", [0, 1, 2]),
            ("ALL", [3, 4, 5]),
            ("ALL", [6, 7, 8]),
            ("ALL", [9]),
        ]
    )


def test_requires_input():
    flow = Dataflow("test_df")
    out = []

    with raises(ValueError):
        run_main(flow)


def test_requires_output():
    flow = Dataflow("test_df")
    inp = range(3)
    flow.input("inp", TestingSource(inp))

    with raises(ValueError):
        run_main(flow)


def test_map():
    flow = Dataflow("test_df")

    inp = [0, 1, 2]
    stream = flow.input("inp", TestingSource(inp))

    def add_one(item):
        return item + 1

    stream = stream.map("add_one", add_one)

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_filter_map():
    flow = Dataflow("test_df")

    inp = [0, 1, 2, 3, 4, 5]
    stream = flow.input("inp", TestingSource(inp))

    def make_odd(item):
        if item % 2 != 0:
            return None
        return item + 1

    stream = stream.filter_map("make_odd", make_odd)

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 3, 5])


def test_flat_map():
    flow = Dataflow("test_df")

    inp = ["split this"]
    stream = flow.input("inp", TestingSource(inp))

    def split_into_words(sentence):
        return sentence.split()

    stream = stream.flat_map("split_into_words", split_into_words)

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(["split", "this"])


def test_filter():
    flow = Dataflow("test_df")

    inp = [1, 2, 3]
    stream = flow.input("inp", TestingSource(inp))

    def is_odd(item):
        return item % 2 != 0

    stream = stream.filter("is_odd", is_odd)

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 3])


def test_inspect():
    flow = Dataflow("test_df")

    inp = ["a"]
    stream = flow.input("inp", TestingSource(inp))

    seen = []
    stream = stream.inspect("insp", seen.append)

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted(["a"])


def test_inspect_epoch():
    flow = Dataflow("test_df")

    inp = ["a"]
    stream = flow.input("inp", TestingSource(inp))

    seen = []
    stream = stream.inspect_epoch(
        "insp", lambda item, epoch: seen.append((epoch, item))
    )

    out = []
    stream.output("out", TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted([(1, "a")])


def test_reduce(recovery_config):
    flow = Dataflow("test_df")

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        "BOOM",
        {"user": "b", "type": "login"},
        {"user": "a", "type": "logout"},
        {"user": "b", "type": "logout"},
    ]
    stream = flow.input("inp", TestingSource(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                msg = "BOOM"
                raise RuntimeError(msg)
            else:
                return []
        else:
            return [item]

    stream = stream.flat_map("trigger", trigger)

    def user_as_key(event):
        return event["user"]

    def wrap_list(event):
        return [event]

    stream = stream.key_on("user_as_key", user_as_key).map_value("wrap_list", wrap_list)

    def extend_session(session, event):
        return session + event

    def session_complete(session):
        return any(event["type"] == "logout" for event in session)

    stream = stream.reduce("sessionizer", extend_session, session_complete)

    out = []
    stream.output("out", TestingSink(out))

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
    flow = Dataflow("test_df")

    inp = [
        "a",
        "b",
        "BOOM",
        "b",
        "c",
        "a",
    ]
    stream = flow.input("inp", TestingSource(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                msg = "BOOM"
                raise RuntimeError(msg)
            else:
                return []
        else:
            return [item]

    stream = stream.flat_map("trigger", trigger)

    def key_on_self(item):
        return item

    stream = stream.key_on("key_on_self", key_on_self)

    def build_seen():
        return set()

    def check(seen, value):
        if value in seen:
            return seen, True
        else:
            seen.add(value)
            return seen, False

    stream = stream.stateful_map("build_seen", build_seen, check)

    def remove_seen(key__is_seen):
        key, is_seen = key__is_seen
        if not is_seen:
            return [key]
        else:
            return []

    stream = stream.flat_map("remove_seen", remove_seen)

    out = []
    stream.output("out", TestingSink(out))

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
    flow = Dataflow("test_df")

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"},
    ]
    stream = flow.input("inp", TestingSource(inp)).assert_keyed("lies")

    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    stream = stream.stateful_map(
        "running_count", lambda: defaultdict(int), running_count
    )

    out = []
    stream.output("out", TestingSink(out))

    expect = "requires `(key, value)` 2-tuple from upstream for routing"

    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)


def test_stateful_map_error_on_non_string_key():
    flow = Dataflow("test_df")

    # Note that the resulting key will be an int.
    inp = [
        {"user": {"id": 1}, "type": "login"},
        {"user": {"id": 1}, "type": "post"},
        {"user": {"id": 2}, "type": "login"},
        {"user": {"id": 2}, "type": "post"},
    ]
    stream = flow.input("inp", TestingSource(inp))

    def add_key(event):
        # Note that event["user"] is an entire dict, but keys must be
        # strings.
        return event["user"]

    stream = stream.key_on("add_key", add_key)

    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    stream = stream.stateful_map(
        "running_count", lambda: defaultdict(int), running_count
    )

    out = []
    stream.output("out", TestingSink(out))

    expect = "requires `str` keys in `(key, value)` from upstream"

    with raises(
        TypeError,
        match=re.escape(expect),
    ):
        run_main(flow)


def test_reduce_window(recovery_config):
    align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    flow = Dataflow("test_df")

    inp = [
        ("ALL", {"time": align_to, "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": 1}),
        "BOOM",
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": 1}),
    ]

    stream = flow.input("inp", TestingSource(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                msg = "BOOM"
                raise RuntimeError(msg)
            else:
                return []
        else:
            return [item]

    stream = stream.flat_map("trigger", trigger).assert_keyed("keyed")

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    def add(acc, x):
        acc["val"] += x["val"]
        return acc

    stream = stream.reduce_window("add", clock_config, window_config, add)

    def extract_val(key__event):
        key, event = key__event
        return (key, event["val"])

    stream = stream.map("extract_val", extract_val)

    out = []
    stream.output("out", TestingSink(out))

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
    flow = Dataflow("test_df")

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

    stream = flow.input("inp", TestingSource(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                msg = "BOOM"
                raise RuntimeError(msg)
            else:
                return []
        else:
            return [item]

    stream = stream.flat_map("trigger", trigger)

    def key_on_user(event):
        return event["user"]

    stream = stream.key_on("key_on_user", key_on_user)

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

    stream = stream.fold_window("count", clock_config, window_config, dict, count)

    out = []
    stream.output("out", TestingSink(out))

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
    flow = Dataflow("test_df")

    inp = [
        ("ALL", {"time": align_to, "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=4), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=8), "val": 1}),
        ("ALL", {"time": align_to + timedelta(seconds=12), "val": 1}),
        "BOOM",
        ("ALL", {"time": align_to + timedelta(seconds=13), "val": 1}),
    ]

    stream = flow.input("inp", TestingSource(inp))

    armed = Event()
    armed.set()

    def trigger(item):
        if item == "BOOM":
            if armed.is_set():
                msg = "BOOM"
                raise RuntimeError(msg)
            else:
                return []
        else:
            return [item]

    stream = stream.flat_map("trigger", trigger).assert_keyed("key")

    clock_config = EventClockConfig(
        lambda e: e["time"], wait_for_system_duration=timedelta(0)
    )
    window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)

    stream = stream.collect_window("add", clock_config, window_config)

    out = []
    stream.output("out", TestingSink(out))

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
