import json

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSink, KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("kafka_in_out")
stream = op.input("inp", flow, KafkaSource(["localhost:9092"], ["input_topic"]))


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    return key, payload


def serialize_with_key(key_payload):
    key, payload = key_payload
    new_key_bytes = key if key else json.dumps("my_key").encode("utf-8")
    return new_key_bytes, json.dumps(payload).encode("utf-8")


stream = op.map("deserialize", stream, deserialize)
op.output("out1", stream, StdOutSink())

stream = op.map("serialize_with_key", stream, serialize_with_key)
op.output(
    "out2",
    stream,
    KafkaSink(
        brokers=["localhost:9092"],
        topic="output_topic",
    ),
)
