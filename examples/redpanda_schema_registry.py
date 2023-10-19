from bytewax.connectors.kafka import (
    ConfluentSchemaRegistry,
    KafkaSink,
    KafkaSource,
    RedpandaSchemaRegistry,
)
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from confluent_kafka import Producer

import logging

logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.DEBUG)


def modify(key_data):
    from random import random
    from time import sleep

    key, data = key_data
    if "initial_value" not in data:
        data["initial_value"] = data["value"]
    data["value"] += 1
    data["diff"] = data["value"] - data["initial_value"]

    # Avoid sending/reading messages as fast as possible
    sleep(random())
    return "ALL", data


def modify2(data):
    return {"a random": "object schema", "because": ["you", "know"]}


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()
    return conf


add_config = read_ccloud_config("client.properties")
# registry = RedpandaSchemaRegistry("sensor-value")
# print(add_config)
registry = ConfluentSchemaRegistry(
    {
        "url": add_config.pop("schema.registry.url"),
        "basic.auth.user.info": add_config.pop("basic.auth.user.info"),
    },
    "sensor_value",
)
flow = Dataflow()
flow.input(
    "redpanda-input",
    KafkaSource(
        ["localhost:19092"],
        topics=["test_topic"],
        schema_registry=registry,
        add_config=add_config,
    ),
)
flow.redistribute()
flow.map("modify", modify2)
flow.inspect(print)
flow.output("stdout", StdOutSink())
flow.output(
    "redpanda-out",
    KafkaSink(
        ["localhost:19092"],
        "test_topic",
        schema_registry=registry,
        add_config=add_config,
    ),
)


# Produce an event to start the loop
print("Producing the first message")
serializer = registry.serde("test_topic")
add_config.pop("basic.auth.credentials.source")
producer = Producer({"bootstrap.servers": "localhost:19092", **add_config})
event = {"timestamp": 212, "identifier": "test", "value": 1}
producer.produce("test_topic", serializer.ser(event))
producer.flush()
