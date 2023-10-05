import json
from datetime import datetime, timedelta, timezone

from bytewax._encoder import encode_dataflow
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource
from bytewax.window import EventClockConfig, TumblingWindow


# Helper functions for some steps
def acc_values(acc, event):
    acc.append((event["value"], event["time"]))
    return acc


# Example class to be encoded
class OrderBook:
    def __init__(self):
        self.data = []

    def update(self, data):
        self.data.append(data)


def test_encoding_custom_object():
    flow = Dataflow()
    flow.stateful_map("avg", OrderBook, OrderBook.update)
    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {
                    "type": "StatefulMap",
                    "builder": "OrderBook",
                    "mapper": "update",
                    "step_id": "avg",
                }
            ],
        },
        sort_keys=True,
    )


def test_encoding_custom_input():
    flow = Dataflow()

    class MyCustomSource(FixedPartitionedSource):
        def list_parts(self):
            return ["one"]

        def build_part(self, for_key, resume_state):
            ...

    flow.input("inp", MyCustomSource())

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {
                    "source": {
                        "type": "MyCustomSource",
                    },
                    "step_id": "inp",
                    "type": "Input",
                },
            ],
        },
        sort_keys=True,
    )


def test_encoding_map():
    flow = Dataflow()
    flow.map("add_one", lambda x: x + 1)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [{"type": "Map", "step_id": "add_one", "mapper": "<lambda>"}],
        },
        sort_keys=True,
    )


def test_encoding_filter():
    flow = Dataflow()
    flow.filter("filter_one", lambda x: x == 1)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {"type": "Filter", "step_id": "filter_one", "predicate": "<lambda>"}
            ],
        },
        sort_keys=True,
    )


def test_encoding_reduce():
    flow = Dataflow()
    flow.reduce("sessionizer", lambda x, y: x + y, lambda x, y: x == y)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {
                    "type": "Reduce",
                    "step_id": "sessionizer",
                    "reducer": "<lambda>",
                    "is_complete": "<lambda>",
                }
            ],
        },
        sort_keys=True,
    )


def test_encoding_flat_map():
    flow = Dataflow()
    flow.flat_map("add_one", lambda x: x + 1)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [{"type": "FlatMap", "step_id": "add_one", "mapper": "<lambda>"}],
        },
        sort_keys=True,
    )


def test_encoding_stateful_map():
    flow = Dataflow()
    flow.stateful_map("order_book", lambda key: OrderBook(), OrderBook.update)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {
                    "builder": "<lambda>",
                    "mapper": "update",
                    "step_id": "order_book",
                    "type": "StatefulMap",
                }
            ],
        },
        sort_keys=True,
    )


def test_encoding_fold_window():
    flow = Dataflow()
    align_to = datetime(2005, 7, 14, 12, 30, tzinfo=timezone.utc)
    wc = TumblingWindow(align_to=align_to, length=timedelta(seconds=5))
    cc = EventClockConfig(
        lambda x: datetime.fromisoformat(x["time"]),
        wait_for_system_duration=timedelta(seconds=10),
    )
    flow.fold_window("running_average", cc, wc, lambda x: list(x), acc_values)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [
                {
                    "builder": "<lambda>",
                    "clock_config": {
                        "dt_getter": "<lambda>",
                        "type": "EventClockConfig",
                        "wait_for_system_duration": "0:00:10",
                    },
                    "folder": "acc_values",
                    "step_id": "running_average",
                    "type": "FoldWindow",
                    "window_config": {
                        "length": "0:00:05",
                        "align_to": "2005-07-14T12:30:00+00:00",
                        "type": "TumblingWindow",
                    },
                }
            ],
        },
        sort_keys=True,
    )


def test_encoding_method_descriptor():
    flow = Dataflow()
    flow.flat_map("split", str.split)

    assert encode_dataflow(flow) == json.dumps(
        {
            "type": "Dataflow",
            "steps": [{"type": "FlatMap", "step_id": "split", "mapper": "split"}],
        },
        sort_keys=True,
    )
