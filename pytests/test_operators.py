import bytewax
import pytest


def test_map():
    out = []

    def add_one(item):
        return item + 1

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, 0),
            (0, 1),
            (0, 2),
        ]
    )
    flow.map(add_one)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted(
        [
            (0, 1),
            (0, 2),
            (0, 3),
        ]
    )


def test_flat_map():
    out = []

    def split_into_words(sentence):
        return sentence.split()

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (1, "split this"),
        ]
    )
    flow.flat_map(split_into_words)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted(
        [
            (1, "split"),
            (1, "this"),
        ]
    )


def test_filter():
    out = []

    def is_odd(item):
        return item % 2 != 0

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, 1),
            (0, 2),
            (0, 3),
        ]
    )
    flow.filter(is_odd)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted([(0, 1), (0, 3)])


def test_inspect():
    out = []

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (1, "a"),
        ]
    )
    flow.inspect(out.append)

    ec.build_and_run()

    assert out == ["a"]


def test_inspect_epoch():
    out = []

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (1, "a"),
        ]
    )
    flow.inspect_epoch(lambda epoch, item: out.append((epoch, item)))

    ec.build_and_run(ctrlc=False)

    assert out == [(1, "a")]


def test_reduce():
    out = []

    def user_as_key(event):
        return (event["user"], [event])

    def extend_session(session, event):
        return session + event

    def session_complete(session):
        return any(event["type"] == "logout" for event in session)

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, {"user": "a", "type": "login"}),
            (1, {"user": "a", "type": "post"}),
            (1, {"user": "b", "type": "login"}),
            (2, {"user": "a", "type": "logout"}),
            (3, {"user": "b", "type": "logout"}),
        ]
    )
    flow.map(user_as_key)
    flow.reduce(extend_session, session_complete)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

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


def test_reduce_epoch():
    out = []

    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, {"user": "a", "type": "login"}),
            (0, {"user": "a", "type": "post"}),
            (0, {"user": "b", "type": "login"}),
            (1, {"user": "b", "type": "post"}),
        ]
    )
    flow.map(add_initial_count)
    flow.reduce_epoch(count)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted(
        [
            (0, ("a", 2)),
            (0, ("b", 1)),
            (1, ("b", 1)),
        ]
    )


def test_reduce_epoch_local():
    # Can't run multiple workers from code yet.
    pass


def test_stateful_map():
    out = []

    def build_seen():
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

    ec = bytewax.Executor()
    flow = ec.Dataflow(
        [
            (0, "a"),
            (0, "a"),
            (1, "a"),
            (1, "b"),
        ]
    )
    flow.map(add_key)
    flow.stateful_map(build_seen, check)
    flow.flat_map(remove_seen)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (1, "b"),
        ]
    )


def test_capture():
    out = []

    inp = [
        (0, "a"),
        (1, "b"),
    ]
    ec = bytewax.Executor()
    flow = ec.Dataflow(inp)
    flow.capture(out.append)

    ec.build_and_run(ctrlc=False)

    assert sorted(out) == sorted(inp)
