import re
from collections import defaultdict

from pytest import raises

from bytewax import Dataflow
from bytewax.execution import run


def test_map():
    def add_one(item):
        return item + 1

    inp = [
        (0, 0),
        (0, 1),
        (0, 2),
    ]

    flow = Dataflow()
    flow.map(add_one)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, 1),
            (0, 2),
            (0, 3),
        ]
    )


def test_flat_map():
    def split_into_words(sentence):
        return sentence.split()

    inp = [
        (1, "split this"),
    ]

    flow = Dataflow()
    flow.flat_map(split_into_words)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (1, "split"),
            (1, "this"),
        ]
    )


def test_filter():
    def is_odd(item):
        return item % 2 != 0

    inp = [
        (0, 1),
        (0, 2),
        (0, 3),
    ]

    flow = Dataflow()
    flow.filter(is_odd)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted([(0, 1), (0, 3)])


def test_inspect():
    inp = [
        (1, "a"),
    ]
    seen = []

    flow = Dataflow()
    flow.inspect(seen.append)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (1, "a"),
        ]
    )
    # Check side-effects after execution is complete.
    assert seen == ["a"]


def test_inspect_epoch():
    inp = [
        (1, "a"),
    ]
    seen = []

    flow = Dataflow()
    flow.inspect_epoch(lambda epoch, item: seen.append((epoch, item)))
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (1, "a"),
        ]
    )
    # Check side-effects after execution is complete.
    assert seen == [(1, "a")]


def test_reduce():
    def user_as_key(event):
        return (event["user"], [event])

    def extend_session(session, event):
        return session + event

    def session_complete(session):
        return any(event["type"] == "logout" for event in session)

    inp = [
        (0, {"user": "a", "type": "login"}),
        (1, {"user": "a", "type": "post"}),
        (1, {"user": "b", "type": "login"}),
        (2, {"user": "a", "type": "logout"}),
        (3, {"user": "b", "type": "logout"}),
    ]

    flow = Dataflow()
    flow.map(user_as_key)
    flow.reduce("sessionizer", extend_session, session_complete)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (
                2,
                (
                    "a",
                    [
                        {"user": "a", "type": "login"},
                        {"user": "a", "type": "post"},
                        {"user": "a", "type": "logout"},
                    ],
                ),
            ),
            (
                3,
                (
                    "b",
                    [
                        {"user": "b", "type": "login"},
                        {"user": "b", "type": "logout"},
                    ],
                ),
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

    inp = [
        (0, "a"),
        (0, "a"),
        (1, "a"),
        (1, "b"),
    ]
    flow = Dataflow()
    flow.map(add_key)
    flow.stateful_map("build_seen", build_seen, check)
    flow.flat_map(remove_seen)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (1, "b"),
        ]
    )


def test_stateful_map_error_on_non_kv_tuple():
    def running_count(type_to_count, event):
        type_to_count[event["type"]] += 1
        current_count = type_to_count[event["type"]]
        return type_to_count, [(event["type"], current_count)]

    inp = [
        (0, {"user": "a", "type": "login"}),
        (0, {"user": "a", "type": "post"}),
        (0, {"user": "b", "type": "login"}),
        (1, {"user": "b", "type": "post"}),
    ]

    flow = Dataflow()
    flow.stateful_map("running_count", lambda key: defaultdict(int), running_count)
    flow.capture()

    expect = (
        "Dataflow requires a `(key, value)` 2-tuple as input to every stateful "
        "operator; got `{'user': 'a', 'type': 'login'}` instead"
    )

    with raises(TypeError, match=re.escape(expect)):
        run(flow, inp)


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
        (0, {"user": {"id": 1}, "type": "login"}),
        (0, {"user": {"id": 1}, "type": "post"}),
        (0, {"user": {"id": 2}, "type": "login"}),
        (1, {"user": {"id": 2}, "type": "post"}),
    ]

    flow = Dataflow()
    flow.map(add_key)
    flow.stateful_map("running_count", lambda key: defaultdict(int), running_count)
    flow.capture()

    with raises(
        TypeError,
        match=re.escape(
            "Stateful operators require string keys in `(key, value)`; "
            "got `{'id': 1}` instead"
        ),
    ):
        run(flow, inp)


def test_capture():
    inp = [
        (0, "a"),
        (1, "b"),
    ]
    out = []

    flow = Dataflow()
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(inp)


def test_capture_multiple():
    inp = [
        (0, "a"),
        (1, "b"),
    ]
    out = []

    flow = Dataflow()
    flow.capture()
    flow.map(str.upper)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (0, "A"),
            (1, "b"),
            (1, "B"),
        ]
    )
