import json

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import StdOutputConfig

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

flow.capture(StdOutputConfig())


if __name__ == "__main__":
    run_main(flow)
