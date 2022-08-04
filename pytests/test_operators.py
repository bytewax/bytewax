import re
from collections import defaultdict

from pytest import raises

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig


def test_map():
    def add_one(item):
        return item + 1

    inp = [0, 1, 2]
    flow = Dataflow(TestingInputConfig(inp))
    flow.map(add_one)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_flat_map():
    def split_into_words(sentence):
        return sentence.split()

    inp = ["split this"]
    flow = Dataflow(TestingInputConfig(inp))
    flow.flat_map(split_into_words)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(["split", "this"])


def test_filter():
    def is_odd(item):
        return item % 2 != 0

    inp = [1, 2, 3]
    flow = Dataflow(TestingInputConfig(inp))
    flow.filter(is_odd)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 3])


def test_inspect():
    inp = ["a"]
    flow = Dataflow(TestingInputConfig(inp))
    seen = []
    flow.inspect(seen.append)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted(["a"])


def test_inspect_epoch():
    inp = ["a"]
    flow = Dataflow(TestingInputConfig(inp))
    seen = []
    flow.inspect_epoch(lambda epoch, item: seen.append((epoch, item)))
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(["a"])
    # Check side-effects after execution is complete.
    assert seen == sorted([(0, "a")])


def test_reduce():
    def user_as_key(event):
        return (event["user"], [event])

    def extend_session(session, event):
        return session + event

    def session_complete(session):
        return any(event["type"] == "logout" for event in session)

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "a", "type": "logout"},
        {"user": "b", "type": "logout"},
    ]
    flow = Dataflow(TestingInputConfig(inp))
    flow.map(user_as_key)
    flow.reduce("sessionizer", extend_session, session_complete)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

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


def test_stateful_map():
    def build_seen(key):
        return set()

    def add_key(item):
        return item, item

    def check(seen, value):
        if value in seen:
            return seen, True
        else:
            seen.add(value)
            return seen, False

    def remove_seen(key__is_seen):
        key, is_seen = key__is_seen
        if not is_seen:
            return [key]
        else:
            return []

    inp = ["a", "a", "a", "b"]
    flow = Dataflow(TestingInputConfig(inp))
    flow.map(add_key)
    flow.stateful_map("build_seen", build_seen, check)
    flow.flat_map(remove_seen)
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(["a", "b"])


def test_stateful_map_error_on_non_kv_tuple():
    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"},
    ]
    flow = Dataflow(TestingInputConfig(inp))
    flow.stateful_map("running_count", lambda key: defaultdict(int), running_count)
    out = []
    flow.capture(TestingOutputConfig(out))

    expect = (
        "Dataflow requires a `(key, value)` 2-tuple as input to every stateful "
        "operator; got `{'user': 'a', 'type': 'login'}` instead"
    )

    with raises(TypeError, match=re.escape(expect)):
        run_main(flow)


def test_stateful_map_error_on_non_string_key():
    def add_key(event):
        # Note that event["user"] is an entire dict, but keys must be
        # strings.
        return event["user"], event

    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    # Note that the resulting key will be an int.
    inp = [
        {"user": {"id": 1}, "type": "login"},
        {"user": {"id": 1}, "type": "post"},
        {"user": {"id": 2}, "type": "login"},
        {"user": {"id": 2}, "type": "post"},
    ]
    flow = Dataflow(TestingInputConfig(inp))
    flow.map(add_key)
    flow.stateful_map("running_count", lambda key: defaultdict(int), running_count)
    out = []
    flow.capture(TestingOutputConfig(out))

    with raises(
        TypeError,
        match=re.escape(
            "Stateful operators require string keys in `(key, value)`; "
            "got `{'id': 1}` instead"
        ),
    ):
        run_main(flow)
