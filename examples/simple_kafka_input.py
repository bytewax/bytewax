import json
from bytewax import cluster_main, Dataflow
from bytewax.inputs import KafkaInputConfig


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes

    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return (key, payload)


def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, (key, payload) = epoch_item
        print(epoch, json.dumps(key), json.dumps(payload))

    return output_handler


if __name__ == "__main__":
    input_config = KafkaInputConfig(
        "localhost:9092", "example_group_id", "drivers", batch_size=5
    )
    flow = Dataflow()
    flow.map(deserialize)
    flow.capture()
    cluster_main(
        flow,
        input_config,
        output_builder,
        [],  # addresses
        0,  # process id
    )
