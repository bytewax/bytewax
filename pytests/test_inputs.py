import datetime

from bytewax import Dataflow, run
from bytewax.inputs import fully_ordered, single_batch, sorted_window, tumbling_epoch


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

    assert sorted(out) == sorted(
        [
            (0, "a"),
            (0, "b"),
            (2, "c"),
        ]
    )


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
