import datetime
from unittest.mock import ANY

from bytewax.inputs import (
    distribute,
    single_batch,
    sorted_window,
    tumbling_epoch,
)


def test_distribute():
    inp = ["a", "b", "c"]

    out1 = distribute(inp, 0, 2)

    assert list(out1) == ["a", "c"]

    out2 = distribute(inp, 1, 2)

    assert list(out2) == ["b"]


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

def test_sorted_window():
    items = [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 4), "value": "b"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 5), "value": "c"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 6), "value": "d"},
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
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 6), "value": "d"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 7), "value": "e"},
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 8), "value": "f"},
    ]
    assert dropped == [
        {"timestamp": datetime.datetime(2022, 2, 22, 1, 2, 3), "value": "a"},
    ]
