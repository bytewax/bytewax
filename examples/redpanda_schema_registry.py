import logging
from random import random
from time import sleep

from bytewax.connectors.kafka import KafkaSink, KafkaSource, RedpandaSchemaRegistry
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
# If a message in the topic is not compatible with the schema,
# the dataflow will crash
registry = RedpandaSchemaRegistry(subject="sensor-value")
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
        topics=["test_topic"],
        schema_registry=registry,
    ),
)

flow.map("modify", modify)
flow.inspect(print)

# Same for the output, if you try to produce a message from a dictionary
# that is not compatible with the schema, the flow will crash
flow.output(
    "redpanda-out",
    KafkaSink(
        ["localhost:19092"],
        # Write to the same topic to test both serialization and
        # deserialization. You will need two instances of the
        # schema registry if the output topic has a different schema.
        "test_topic",
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
