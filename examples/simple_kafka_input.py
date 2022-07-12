import json
import datetime
from bytewax import cluster_main, Dataflow
from bytewax.inputs import KafkaInputConfig, BatchInputPartitionerConfig, TumblingWindowInputPartitionerConfig

def deserialize(payload_bytes):
    try:
        payload = json.loads(payload_bytes) if payload_bytes else None
        return payload
    except json.JSONDecodeError as e:
        print("Invalid json", e)
        # Hack to circumvent err 
        return json.loads('{"driver_id": 1003, "event_timestamp": "2022-03-14 14:00:00", "conv_rate": 0.8699171543121338, "acc_rate": 0.7795618176460266, "created": "2022-03-14 15:21:25"}')

def filter_invalid_json_err(item):
    return item

def get_time(item):
    return datetime.datetime.fromisoformat(item[0]["event_timestamp"])

def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, payload = epoch_item
        print(epoch, json.dumps(payload))

    return output_handler


if __name__ == "__main__":
    input_config = KafkaInputConfig(
        "localhost:9092", "example_group_id", "drivers", deserializer=deserialize
    )
    partition_config = TumblingWindowInputPartitionerConfig(
        window_length=datetime.timedelta(hours=1),
        window_start_time=datetime.datetime.strptime("2022-03-06 02:00:00", "%Y-%m-%d %H:%M:%S"),
        time_getter = get_time
    )
    flow = Dataflow()
    flow.capture()
    cluster_main(
        flow,
        input_config,
        output_builder,
        [],  # addresses
        0,  # process id
        input_partitioner_config=partition_config,
    )
