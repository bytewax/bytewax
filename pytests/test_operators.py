from collections import defaultdict

from bytewax import Dataflow, run, run_cluster
from bytewax.recovery import SqliteRecoveryConfig


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


def test_reduce_recovery(tmp_path):
    recovery_config = SqliteRecoveryConfig(str(tmp_path / "state.sqlite3"), create=True)

    inp1 = [
        (0, ("blake", 5)),
        (1, ("dan", 3)),
        (2, ("blake", 5)),
    ]

    def is_10(total_points):
        return total_points >= 10

    def sum_total(total_points, points):
        return total_points + points

    flow = Dataflow()
    flow.reduce("sum", sum_total, is_10)
    flow.capture()

    out1 = run(flow, inp1, recovery_config=recovery_config)

    assert sorted(out1) == sorted(
        [
            (2, ("blake", 10)),
        ]
    )

    # Simulate restart a little back at epoch 2.

    inp2 = [
        (2, ("blake", 10)),
        (3, ("dan", 8)),
    ]

    out2 = run(flow, inp2, recovery_config=recovery_config)

    assert sorted(out2) == sorted(
        [
            (2, ("blake", 15)),
            (3, ("dan", 11)),
        ]
    )


def test_reduce_epoch():
    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count

    inp = [
        (0, {"user": "a", "type": "login"}),
        (0, {"user": "a", "type": "post"}),
        (0, {"user": "b", "type": "login"}),
        (1, {"user": "b", "type": "post"}),
    ]

    flow = Dataflow()
    flow.map(add_initial_count)
    flow.reduce_epoch(count)
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, ("a", 2)),
            (0, ("b", 1)),
            (1, ("b", 1)),
        ]
    )


def test_reduce_epoch_local():
    def add_initial_count(event):
        return event["user"], 1

    def count(count, event_count):
        return count + event_count

    inp = [
        (0, {"user": "a", "type": "login"}),
        (0, {"user": "a", "type": "post"}),
        (0, {"user": "a", "type": "post"}),
        (0, {"user": "b", "type": "login"}),
        (1, {"user": "b", "type": "post"}),
        (1, {"user": "b", "type": "post"}),
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


def test_stateful_map_recovery(tmp_path):
    recovery_config = SqliteRecoveryConfig(str(tmp_path / "state.sqlite3"), create=True)

    inp1 = [
        (0, ("a", 8)),
        (1, ("b", 2)),
        (2, ("a", 5)),
    ]

    def keep_max(previous_max, new_item):
        if previous_max is None:
            new_max = new_item
        else:
            new_max = max(previous_max, new_item)
        return new_max, new_max

    flow = Dataflow()
    flow.stateful_map("max", lambda key: None, keep_max)
    flow.capture()

    out1 = run(flow, inp1, recovery_config=recovery_config)

    assert sorted(out1) == sorted(
        [
            (0, ("a", 8)),
            (1, ("b", 2)),
            (2, ("a", 8)),
        ]
    )

    inp2 = [
        (2, ("a", 15)),
        (3, ("b", 1)),
    ]

    out2 = run(flow, inp2, recovery_config=recovery_config)

    assert sorted(out2) == sorted(
        [
            (2, ("a", 15)),
            (3, ("b", 2)),
        ]
    )


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
