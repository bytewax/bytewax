import json
from bytewax import cluster_main, Dataflow
from bytewax.inputs import KafkaInputConfig


def output_builder(worker_index, worker_count):
    def output_fn(epoch_keypayload):
        e, data = epoch_keypayload
        # Alert! Data items can be None
        payload = bytearray(data[1]) if data[1] else ""
        msg = json.loads(payload)
        print(e, msg)

    return output_fn


if __name__ == "__main__":
    input_config = KafkaInputConfig("localhost:9092", "foobar", "drivers", batch_size=5)
    flow = Dataflow()
    flow.capture()
    cluster_main(
        flow,
        input_config,
        output_builder,
        [],  # addresses
        0,  # process id
    )
