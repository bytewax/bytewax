import logging
from datetime import timedelta

from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSource,
    RedpandaSchemaRegistry,
    SchemaConf,
)
from bytewax.dataflow import Dataflow

logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.DEBUG)


flow = Dataflow()

# Instantiate a schema registry object.
registry = RedpandaSchemaRegistry(
    # Redpanda's schema registry requires to specify both
    # serialization and deserialization schemas
    # SchemaConf is used to determine the specific schema.
    input_value_conf=SchemaConf(subject="sensor-value"),
    input_key_conf=SchemaConf(subject="sensor-key"),
    output_value_conf=SchemaConf(subject="aggregated-value"),
    output_key_conf=SchemaConf(subject="sensor-key"),
)
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

flow.input(
    "redpanda-input",
    KafkaSource(
        ["localhost:19092"],
        topics=["in_topic"],
        schema_registry=registry,
    ),
)

# Print the message as it arrives
flow.inspect(lambda x: print(f"Read: {x}"))
# Extract the identifier from the key
flow.map("key", lambda x: (x[0]["identifier"], x))
# Batch every second
flow.batch("batch", max_size=100000, timeout=timedelta(seconds=1))


def calc_avg(key__batch):
    key, batch = key__batch
    schema_key = batch[0][0]
    avg = int(1000 * sum([i[1].value["value"] for i in batch]) / len(batch))
    return (schema_key, {"identifier": key, "avg": avg})


flow.map("avg", calc_avg)
flow.inspect(lambda x: print(f"Writing: {x}"))

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
