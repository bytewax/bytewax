import json
from bytewax import cluster_main, Dataflow
from bytewax.inputs import KafkaInputConfig, BatchInputPartitionerConfig

def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    try:
        key = json.loads(key_bytes) if key_bytes else None
        payload = json.loads(payload_bytes) if payload_bytes else None
        return [(key, payload)]
    except json.JSONDecodeError as e:
        print("Invalid json", e)
        return []

def filter_invalid_json_err(item):
    return item

def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, (key, payload) = epoch_item
        print(epoch, json.dumps(key), json.dumps(payload))

    return output_handler


if __name__ == "__main__":
    input_config = KafkaInputConfig(
        "localhost:9092", "example_group_id", "drivers", deserializer=deserialize
    )
    flow = Dataflow()
    flow.flat_map(filter_invalid_json_err)
    flow.capture()
    cluster_main(
        flow,
        BatchInputPartitionerConfig(),
        input_config,
        output_builder,
        [],  # addresses
        0,  # process id
    )
