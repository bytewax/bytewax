import logging
from datetime import timedelta
from random import random
from time import sleep

from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    RedpandaSchemaRegistry,
    SchemaConf,
)
from bytewax.dataflow import Dataflow

logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.DEBUG)


def modify(key_msg):
    key, msg = key_msg
    data = msg.value
    data["value"] += 1

    # If you make the message incompatible with the schema,
    # the dataflow will crash at the output
    # del data["timestamp"]

    # Avoid sending/reading messages as fast as possible
    sleep(random())
    return "ALL", msg


# Instantiate a schema registry object.
registry = RedpandaSchemaRegistry(
    # Redpanda's schema registry requires to specify both
    # serialization and deserialization schemas
    input_value_conf=SchemaConf(subject="sensor-value"),
    input_key_conf=SchemaConf(subject="sensor-key"),
    output_value_conf=SchemaConf(subject="aggregated-value"),
    output_key_conf=SchemaConf(subject="sensor-key"),
)
# By default, the client will fetch the latest version.
# You can also specify the version:
# >>> registry = RedpandaSchemaRegistry(subject="sensor-value", version=1)
#
# Or, if you have it, the schema_id:
# >>> registry = RedpandaSchemaRegistry(schema_id=10001)
#
# Alternatively, you can use ConfluentSchemaRegistry:
# >>> registry = ConfluentSchemaRegistry(
# ...    schema_id=10001,
# ...    conf={
# ...        "bootstrap.servers": "...",
# ...        "sasl.username": "...",
# ...        "sasl.password": "...",
# ...        "url": "...",
# ...        "basic.auth.user.info": "...",
# ...    },
# ... )

flow = Dataflow()
flow.input(
    "redpanda-input",
    KafkaSource(
        ["localhost:19092"],
        topics=["in_topic"],
        schema_registry=registry,
    ),
)

flow.inspect(print)
flow.map("key", lambda x: (x[0]["identifier"], x))
flow.batch("batch", max_size=100000, timeout=timedelta(seconds=1))
flow.map(
    "avg",
    lambda x: (
        x[1][0][0],
        {
            "identifier": x[0],
            "avg": int(1000 * sum([i[1].value["value"] for i in x[1]]) / len(x[1])),
        },
    ),
)
flow.inspect(print)

# Same for the output, if you try to produce a message from a dictionary
# that is not compatible with the schema, the flow will crash
flow.output(
    "redpanda-out",
    KafkaSink(
        ["localhost:19092"],
        "out_topic",
        schema_registry=registry,
    ),
)


# Produce an event to start the loop
# from confluent_kafka import Producer
# print("Producing the first message")
# serializer = registry.serde("test_topic")
# add_config.pop("basic.auth.credentials.source")
# producer = Producer({"bootstrap.servers": "localhost:19092", **add_config})
# event = {"timestamp": 212, "identifier": "test", "value": 1}
# producer.produce("test_topic", serializer.ser(event))
# producer.flush()
