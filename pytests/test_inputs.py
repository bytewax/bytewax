import datetime

from bytewax import Dataflow, run
from bytewax.inputs import fully_ordered, single_batch, tumbling_epoch, order


def test_single_batch():
    inp = single_batch(["a", "b", "c"])

    flow = Dataflow()
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (0, "b"),
            (0, "c"),
        ]
    )


def test_order():
    items = [
        (2, "c"),
        (1, "b"),
        (0, "a"),
        (3, "d"),
        (4, "e"),
    ]
    out = order(items, 2)

    assert list(out) == sorted(items)


def test_order_drops_too_late():
    items = [
        (2, "c"),
        (1, "b"),
        (0, "a"),
    ]

    dropped = []
    out = order(items, 1, on_drop = dropped.append)

    assert list(out) == [
        (1, "b"),
        (2, "c"),
    ]
    assert dropped == [(0, "a")]


def test_tumbling_epoch():
    def time_getter(item):
        return item["timestamp"]

    items = [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "c"},
    ]
    inp = tumbling_epoch(
        items,
        datetime.timedelta(seconds=2),
        time_getter,
    )

    flow = Dataflow()
    flow.map(lambda item: item["value"])
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted([
        (0, "a"),
        (0, "b"),
        (2, "c"),
    ])


def test_fully_ordered():
    inp = fully_ordered(["a", "b", "c"])

    flow = Dataflow()
    flow.capture()

    out = run(flow, inp)

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (1, "b"),
            (2, "c"),
        ]
    )
