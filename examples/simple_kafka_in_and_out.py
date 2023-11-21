import json

from bytewax.connectors.kafka import KafkaMessage, KafkaSink, KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("inp", KafkaSource(["localhost:9092"], ["input_topic"]))


def deserialize(msg: KafkaMessage):
    key = json.loads(msg.key) if msg.key else None
    payload = json.loads(msg.value) if msg.value else None
    return key, payload


def serialize_with_key(key_payload):
    key, payload = key_payload
    new_key_bytes = key if key else json.dumps("my_key").encode("utf-8")
    return new_key_bytes, json.dumps(payload).encode("utf-8")


flow.map("deserialize", deserialize)
flow.output("out", StdOutSink())

flow.map("serialize_with_key", serialize_with_key)
flow.output(
    "out",
    KafkaSink(
        brokers=["localhost:9092"],
        topic="output_topic",
    ),
)
