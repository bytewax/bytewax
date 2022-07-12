import re
from collections import defaultdict

from pytest import raises

from bytewax import Dataflow, run, run_cluster
from bytewax.inputs import BatchInputPartitionerConfig


def test_map():
    def add_one(item):
        return item + 1

    inp = [0, 1, 2]

    flow = Dataflow()
    flow.map(add_one)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, 1),
            (1, 2),
            (2, 3),
        ]
    )


def test_flat_map():
    def split_into_words(sentence):
        return sentence.split()

    inp = ["split this"]

    flow = Dataflow()
    flow.flat_map(split_into_words)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "split"),
            (0, "this"),
        ]
    )


def test_filter():
    def is_odd(item):
        return item % 2 != 0

    inp = [1,2,3]

    flow = Dataflow()
    flow.filter(is_odd)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted([(0, 1), (2, 3)])


def test_inspect():
    inp = ["a"]
    seen = []

    flow = Dataflow()
    flow.inspect(seen.append)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
        ]
    )
    # Check side-effects after execution is complete.
    assert seen == ["a"]


def test_inspect_epoch():
    inp = ["a"]
    seen = []

    flow = Dataflow()
    flow.inspect_epoch(lambda epoch, item: seen.append((epoch, item)))
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
        ]
    )
    # Check side-effects after execution is complete.
    assert seen == [(0, "a")]


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
        {"user": "b", "type": "logout"}
    ]

    flow = Dataflow()
    flow.map(user_as_key)
    flow.reduce("sessionizer", extend_session, session_complete)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (
                3,
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
                4,
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
    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count


    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"}
    ]

    flow = Dataflow()
    flow.map(add_initial_count)
    flow.reduce_epoch(count)
    flow.capture()

    # Will assign 0 epoch to first three items, 1 different to the last
    partition_config = BatchInputPartitionerConfig(3)
    out = run(flow, inp, input_partitioner_config=partition_config)

    assert sorted(out) == sorted(
        [
            (0, ("a", 2)),
            (0, ("b", 1)),
            (1, ("b", 1)),
        ]
    )


def test_reduce_epoch_error_on_non_kv_tuple():
    def count(count, event_count):
        return count + event_count

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"}
    ]

    flow = Dataflow()
    flow.reduce_epoch(count)
    flow.capture()

    expect = (
        "Dataflow requires a `(key, value)` 2-tuple as input to every stateful "
        "operator; got `{'user': 'a', 'type': 'login'}` instead"
    )

    with raises(TypeError, match=re.escape(expect)):
        run(flow, inp)


def test_reduce_epoch_error_on_non_string_key():
    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count

    # Note that the resulting key will be an int.
    inp = [
        {"user": 1, "type": "login"},
        {"user": 1, "type": "post"},
        {"user": 2, "type": "login"},
        {"user": 2, "type": "post"}
    ]

    flow = Dataflow()
    flow.map(add_initial_count)
    flow.reduce_epoch(count)
    flow.capture()

    with raises(
        TypeError,
        match=re.escape(
            "Stateful operators require string keys in `(key, value)`; got `1` instead"
        ),
    ):
        run(flow, inp)


def test_reduce_epoch_local():
    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count

    inp = [
        {"user": "a", "type": "login"},
        {"user": "a", "type": "post"},
        {"user": "b", "type": "login"},
        {"user": "b", "type": "post"}
    ]

    flow = Dataflow()
    flow.map(add_initial_count)
    flow.reduce_epoch_local(count)
    flow.capture()

    workers = 2
    out = run_cluster(flow, inp, proc_count=workers)

    # out should look like (epoch, (user, event_count)) per worker. So
    # if we count the number of output items that have a given (epoch,
    # user), we should get some that the counts == number of workers.
    epoch_user_to_count = defaultdict(int)
    for epoch, user_count in out:
        user, count = user_count
        epoch_user_to_count[(epoch, user)] += 1

    assert workers in set(epoch_user_to_count.values())


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

    partition_config = BatchInputPartitionerConfig(2)

    flow = Dataflow()
    flow.map(add_key)
    flow.stateful_map("build_seen", build_seen, check)
    flow.flat_map(remove_seen)
    flow.capture()

    out = run(flow, inp, input_partitioner_config=partition_config)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (1, "b"),
        ]
    )


def test_capture():
    inp = ["a", "b"]
    out = []

    flow = Dataflow()
    flow.capture()

    out = run(flow, inp)
    assert sorted(out) == sorted(
        [
            (0, "a"),
            (1, "b"),
        ]
    )


def test_capture_multiple():
    inp = ["a", "b"]
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
