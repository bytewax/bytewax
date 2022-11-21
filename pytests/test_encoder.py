import pytest
import json

from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
from bytewax.encoder import encode_dataflow
from bytewax.inputs import ManualInputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig

from unittest import TestCase

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


@pytest.mark.parametrize(
    ["step", "output"],
    [
        (
            "manual_input",
            {
                "input_config": {
                    "input_builder": "<lambda>",
                    "type": "ManualInputConfig",
                },
                "step_id": "inp",
                "type": "Input",
            },
        ),
        ("map", {"type": "Map", "mapper": "<lambda>"}),
        ("flatmap", {"type": "FlatMap", "mapper": "<lambda>"}),
        (
            "reduce",
            {
                "type": "Reduce",
                "step_id": "sessionizer",
                "reducer": "<lambda>",
                "is_complete": "<lambda>",
            },
        ),
        ("filter", {"type": "Filter", "predicate": "<lambda>"}),
        (
            "fold_window",
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
                    "start_at": "2005-07-14T12:30:00+00:00",
                    "type": "TumblingWindowConfig",
                },
            },
        ),
        (
            "stateful_map",
            {
                "builder": "<lambda>",
                "mapper": "update",
                "step_id": "order_book",
                "type": "StatefulMap",
            },
        ),
        ("method_descriptor", {"type": "Map", "mapper": "split"}),
    ],
)
def test_dataflow_encoding(step, output):
    flow = Dataflow()
    inp = [0, 1, 2]
    if step == "manual_input":
        flow.input("inp", ManualInputConfig(lambda: inp))
    elif step == "map":
        flow.map(lambda x: x + 1)
    elif step == "method_descriptor":
        flow.map(str.split)
    elif step == "stateful_map":
        flow.stateful_map("order_book", lambda key: OrderBook(), OrderBook.update)
    elif step == "flatmap":
        flow.flat_map(lambda x: x + 1)
    elif step == "filter":
        flow.filter(lambda x: x == 1)
    elif step == "reduce":
        flow.reduce("sessionizer", lambda x, y: x + y, lambda x, y: x == y)
    elif step == "fold_window":
        start_at = datetime(2005, 7, 14, 12, 30).replace(tzinfo=timezone.utc)
        wc = TumblingWindowConfig(start_at=start_at, length=timedelta(seconds=5))
        cc = EventClockConfig(
            lambda x: datetime.fromisoformat(x["time"]),
            wait_for_system_duration=timedelta(seconds=10),
        )
        flow.fold_window("running_average", cc, wc, lambda x: list(x), acc_values)

    assert encode_dataflow(flow) == json.dumps(
        {
            "Dataflow": {
                "steps": [
                    output,
                ]
            }
        },
        sort_keys=True,
    )
