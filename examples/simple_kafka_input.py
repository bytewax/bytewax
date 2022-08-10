import json

from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import spawn_cluster
from bytewax.outputs import ManualEpochOutputConfig

flow = Dataflow()

flow.input(
    "inp",
    KafkaInputConfig(
        brokers=["localhost:9092"],
        topic="input_test",
    ),
)


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return key, payload


flow.map(deserialize)


def filter_invalid_json_err(item):
    return item


def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, (key, payload) = epoch_item
        print(epoch, json.dumps(key), json.dumps(payload))
    return output_handler


flow.capture(StdOutputConfig())


if __name__ == "__main__":
    input_config = KafkaInputConfig(
        "localhost:9092", "example_group_id", "drivers"
    )
    flow = Dataflow(input_config)
    flow.map(deserialize)
    flow.flat_map(filter_invalid_json_err)
    flow.capture(ManualEpochOutputConfig(output_builder))
    spawn_cluster(flow)
