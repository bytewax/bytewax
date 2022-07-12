import json
import datetime
from bytewax import cluster_main, Dataflow
from bytewax.inputs import ManualInputConfig, TumblingWindowInputPartitionerConfig

def input_builder(worker_index, worker_count, resume_epoch):
    with open("examples/sample_data/simple.json") as f:
        for line in f:
            yield line


def get_time(item):
    return datetime.datetime.fromisoformat(item["event_timestamp"])

def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, message = epoch_item
        print(epoch, json.dumps(message))

    return output_handler


if __name__ == "__main__":
    input_config=ManualInputConfig(input_builder, json.loads)
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
