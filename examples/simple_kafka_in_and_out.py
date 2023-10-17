import json
from typing import Dict, Tuple

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaMessage, KafkaSink, KafkaSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("kafka_in_out")
inp = op.input("inp", flow, KafkaSource(["localhost:9092"], ["input_topic"]))


def deserialize(msg: KafkaMessage) -> Tuple[Dict, Dict]:
    key = json.loads(msg.key)
    payload = json.loads(msg.value)
    return key, payload


deserialized = op.map("deserialize", inp, deserialize)
op.output("out1", deserialized, StdOutSink())


def serialize_with_key(key_payload: Tuple[Dict, Dict]) -> Tuple[bytes, bytes]:
    key, payload = key_payload
    serialized_key = json.dumps(key).encode("utf-8")
    serialized_payload = json.dumps(payload).encode("utf-8")
    return serialized_key, serialized_payload


serialized = op.map("serialize_with_key", deserialized, serialize_with_key)
op.output(
    "out2",
    serialized,
    KafkaSink(
        brokers=["localhost:9092"],
        topic="output_topic",
    ),
)
