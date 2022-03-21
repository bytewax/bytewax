import datetime

from bytewax import Dataflow, run
from bytewax.inputs import fully_ordered, single_batch, sorted_window, tumbling_epoch


def test_single_batch():
    out = single_batch(["a", "b", "c"])

    assert list(out) == [
        (0, "a"),
        (0, "b"),
        (0, "c"),
    ]


def test_tumbling_epoch():
    def time_getter(item):
        return item["timestamp"]

    items = [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "c"},
    ]
    out = tumbling_epoch(
        items,
        datetime.timedelta(seconds=2),
        time_getter,
    )

    assert list(out) == [
        (0, {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"}),
        (0, {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"}),
        (2, {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "c"}),
    ]


def test_fully_ordered():
    out = fully_ordered(["a", "b", "c"])

    assert list(out) == [
        (0, "a"),
        (1, "b"),
        (2, "c"),
    ]


def test_sorted_window():
    items = [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 5), "value": "c"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 6), "value": "d"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "f"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 7), "value": "e"},
    ]

    dropped = []
    out = sorted_window(
        items,
        datetime.timedelta(seconds=2),
        lambda item: item["timestamp"],
        on_drop=dropped.append,
    )

    assert list(out) == [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 5), "value": "c"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 6), "value": "d"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 7), "value": "e"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "f"},
    ]
    assert dropped == [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"},
    ]
