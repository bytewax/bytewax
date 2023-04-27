import json

from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow


flow = Dataflow()
flow.input("inp", KafkaInput(["localhost:9092"], ["input_topic"]))


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return key, payload


def serialize_with_key(key_payload):
    key, payload = key_payload
    new_key_bytes = key if key else json.dumps("my_key").encode("utf-8")
    return new_key_bytes, json.dumps(payload).encode("utf-8")


flow.map(deserialize)
flow.output("out", StdOutput())

flow.map(serialize_with_key)
flow.output(
    "out",
    KafkaOutput(
        brokers=["localhost:9092"],
        topic="output_topic",
    )
)
