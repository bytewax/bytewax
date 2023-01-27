import json

from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import MqttInputConfig
from bytewax.outputs import MqttOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


HOST = "tcp://broker.emqx.io:1883"


def validate_and_load(data):
    """Filter and make only valid data pass"""
    try:
        data = json.loads(data)
        assert "type" in data, "Missing 'type' field"
        assert "value" in data, "Missing 'value' field"
        assert data["value"] is not None, "'value' field is None"
        return data["type"], [data["value"]]
    except Exception as e:
        print(f"Received invalid data, filtered: {e}")
        return None


def accumulate(acc, data):
    return [*acc, *data]


def avg(key__data):
    key, data = key__data
    return key, sum(data) / len(data)


flow = Dataflow()
flow.input(
    "input",
    MqttInputConfig(
        host=HOST, topics=["rust/input"], qos=[0], client_id="rust_subscribe"
    ),
)
flow.inspect(print)
flow.filter_map(validate_and_load)
flow.reduce_window(
    "avg",
    SystemClockConfig(),
    TumblingWindowConfig(length=timedelta(seconds=5)),
    accumulate,
)
flow.map(avg)
flow.capture(MqttOutputConfig(topic="rust/output", host=HOST))


if __name__ == "__main__":
    spawn_cluster(flow)
