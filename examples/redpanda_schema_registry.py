from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    RedpandaSchemaRegistry,
    ConfluentSchemaRegistry,
)

# from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from confluent_kafka import Producer


def modify(key_data):
    from random import random
    from time import sleep

    key, data = key_data
    data["value"] += 1

    # Avoid sending/reading messages as fast as possible
    sleep(random())
    return "ALL", data


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
    "test_topic",
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
flow.map("modify", modify)
flow.inspect(print)
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
serializer = registry.serializer()
add_config.pop("basic.auth.credentials.source")
producer = Producer({"bootstrap.servers": "localhost:19092", **add_config})
event = {"timestamp": 212, "identifier": "test", "value": 123}
producer.produce("test_topic", serializer.encode(event))
producer.flush()
