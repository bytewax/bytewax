import json

from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import KafkaOutputConfig, StdOutputConfig

flow = Dataflow()

flow.input(
    "inp",
    KafkaInputConfig(
        brokers=["localhost:9092"],
        topic="input_topic",
    ),
)

def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return key, payload

def serialize_with_key(key_payload):
    key, payload = key_payload
    new_key_bytes = key if key else json.dumps("my_key").encode('utf-8')
    return new_key_bytes, json.dumps(payload).encode('utf-8')

flow.map(deserialize)
flow.capture(StdOutputConfig())

flow.map(serialize_with_key)
flow.capture(
    KafkaOutputConfig(
        brokers=["localhost:9092"],
        topic="output_topic",
    )
)

if __name__ == "__main__":
    run_main(flow)
