import logging
import os
from datetime import timedelta

from bytewax.connectors.kafka import (
    ConfluentSchemaRegistry,
    KafkaSink,
    KafkaSource,
    # RedpandaSchemaRegistry,
    SchemaConf,
)
from bytewax.dataflow import Dataflow

logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.DEBUG)

KAFKA_SERVER = os.environ["KAFKA_SERVER"]


flow = Dataflow()

# Confluent's schema registry configuration
CONFLUENT_URL = os.environ["CONFLUENT_URL"]
CONFLUENT_USERINFO = os.environ["CONFLUENT_USERINFO"]
CONFLUENT_USERNAME = os.environ["CONFLUENT_USERNAME"]
CONFLUENT_PASSWORD = os.environ["CONFLUENT_PASSWORD"]
registry = ConfluentSchemaRegistry(
    sr_conf={
        "url": os.environ["CONFLUENT_URL"],
        "basic.auth.user.info": os.environ["CONFLUENT_USERINFO"],
    },
    key_conf=SchemaConf(schema_id=100002),
    value_conf=SchemaConf(schema_id=100003),
)

# # Alternatively, we support Redpanda's schema registry
# registry = RedpandaSchemaRegistry(
#     # Redpanda's schema registry requires to specify both
#     # serialization and deserialization schemas
#     # SchemaConf is used to determine the specific schema.
#     input_value_conf=SchemaConf(subject="sensor-value"),
#     input_key_conf=SchemaConf(subject="sensor-key"),
#     output_value_conf=SchemaConf(subject="aggregated-value"),
#     output_key_conf=SchemaConf(subject="sensor-key"),
# )


flow.input(
    "schema-input",
    KafkaSource(
        [KAFKA_SERVER],
        topics=["in_topic"],
        schema_registry=registry,
        # Working with confluent's cloud
        add_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": CONFLUENT_USERNAME,
            "sasl.password": CONFLUENT_PASSWORD,
        },
    ),
)

# Print the message as it arrives
flow.inspect(lambda x: print(f"Read: {x}"))
# Extract the identifier from the key
flow.map("key", lambda x: (x[0]["identifier"], x))
# Batch every second
flow.batch("batch", max_size=100000, timeout=timedelta(seconds=1))


def calc_avg(key__batch):
    # Some dubious calculations
    key, batch = key__batch
    schema_key = batch[0][0]
    avg = int(1000 * sum([i[1].value["value"] for i in batch]) / len(batch))
    # We need to use the proper schema here
    return (schema_key, {"identifier": key, "avg": avg})


flow.map("avg", calc_avg)
flow.inspect(lambda x: print(f"Writing: {x}"))

# Same for the output, if you try to produce a message from a dictionary
# that is not compatible with the schema, the flow will crash
flow.output(
    "schema-output",
    KafkaSink(
        [KAFKA_SERVER],
        "out_topic",
        schema_registry=registry,
        add_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": CONFLUENT_USERNAME,
            "sasl.password": CONFLUENT_PASSWORD,
        },
    ),
)
