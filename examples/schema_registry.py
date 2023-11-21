"""
Schema registry complete example.

The Kafka input connector has support for schema registries.
We support Redpanda and Confluent registries.
This example shows how to use the clients in the kafka connector.

The schems used for this example are the following:

- Subject `sensor-key`:
    {
        "type": "record",
        "name": "sensor_key",
        "fields": [
            {"name": "identifier", "type": "string", "logicalType": "uuid"},
            {"name": "name", "type": "string"},
        ],
    }

- Subject `sensor-value`:
    {
        "type": "record",
        "name": "sensor_sample",
        "fields": [
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "identifier", "type": "string", "logicalType": "uuid"},
            {"name": "value", "type": "long"},
        ],
    }

- Subject `aggregated-value`:
    {
        "type": "record",
        "name": "aggregated_sensor",
        "fields": [
            {"name": "identifier", "type": "string", "logicalType": "uuid"},
            {"name": "avg", "type": "long"},
            {"name": "window_start", "type": "string"},
            {"name": "window_end", "type": "string"},
        ],
    }

"""
import logging
import os  # noqa F401
from datetime import datetime, timedelta, timezone

from bytewax.connectors.kafka import (
    ConfluentSchemaRegistry,  # noqa F401
    KafkaMessage,
    KafkaSink,
    KafkaSource,
    RedpandaSchemaRegistry,
    SchemaConf,
)
from bytewax.dataflow import Dataflow
from bytewax.window import SystemClockConfig, TumblingWindow

logger = logging.getLogger(__name__)
logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.WARNING)


flow = Dataflow()

# Redpanda's schema registry
KAFKA_SERVER = "localhost:19092"
# The schema registry object is used to retrieve and parse
# schemas from the registry.
# You specify which schema you want with a `SchemaConf` object.
# You can specify either the `schema_id` or `subject`+`version`
registry = RedpandaSchemaRegistry(
    # All the schemas are optional. Here we define them all though.
    # If a schema is not passed, (de)serialization won't be enabled.
    key_deserialization_schema=SchemaConf(subject="sensor-key"),
    value_deserialization_schema=SchemaConf(subject="sensor-value"),
    # We can reuse the same schema for the key
    key_serialization_schema=SchemaConf(subject="sensor-key"),
    value_serialization_schema=SchemaConf(subject="aggregated-value"),
)

# # Confluent's schema registry configuration
# # You need to setup some env vars first:
# KAFKA_SERVER = os.environ["KAFKA_SERVER"]
# CONFLUENT_URL = os.environ["CONFLUENT_URL"]
# CONFLUENT_USERINFO = os.environ["CONFLUENT_USERINFO"]
# CONFLUENT_USERNAME = os.environ["CONFLUENT_USERNAME"]
# CONFLUENT_PASSWORD = os.environ["CONFLUENT_PASSWORD"]
# import confluent_kafka

# registry = ConfluentSchemaRegistry(
#     sr_conf={
#         "url": os.environ["CONFLUENT_URL"],
#         "basic.auth.user.info": os.environ["CONFLUENT_USERINFO"],
#     },
#     key_conf=SchemaConf(schema_id=100002),  # Use the proper ID here
#     value_conf=SchemaConf(schema_id=100003),  # Use the proper ID here
# )


flow.input(
    "schema-input",
    KafkaSource(
        [KAFKA_SERVER],
        topics=["in_topic"],
        schema_registry=registry,
        # We can decide to not crash the dataflow if a deserialization
        # error occurs. We'll have to handle errors ourselves later.
        raise_on_deserialization_error=False,
        # # For ConfluentSchemaRegistry add:
        # add_config={
        #     "security.protocol": "SASL_SSL",
        #     "sasl.mechanism": "PLAIN",
        #     "sasl.username": CONFLUENT_USERNAME,
        #     "sasl.password": CONFLUENT_PASSWORD,
        # },
    ),
)

# Print the message as it arrives
flow.inspect(lambda x: print(f"Read: {x}"))


def remove_errors(msg: KafkaMessage):
    # Since we are not raising an exception on serialization errors,
    # we need to check if something went wrong, and discard the messages.
    # We could send them to a dead letter queue here.
    if msg.key_error or msg.value_error:
        logger.warning("key_error: %s value_error: %s", msg.key_error, msg.value_error)
        return None
    return msg


flow.filter_map("remove_errors", remove_errors)

# Use the "identifier" field of the key as bytewax's key
def extract_identifier(msg: KafkaMessage):
    return (msg.key["identifier"], msg)


flow.map("key_on_identifier", extract_identifier)

# Let's window the input
cc = SystemClockConfig()
wc = TumblingWindow(timedelta(seconds=1), datetime(2023, 1, 1, tzinfo=timezone.utc))


# Accumulate data in a list
def accumulate(acc, msg: KafkaMessage):
    acc.append(msg.value["value"])
    return acc


flow.fold_window("calc_avg", cc, wc, list, accumulate)


# Calc the average for the window
def calc_avg(key__wm__batch):
    # wm is a WindowMetadata object.
    # You can inspect the window with
    # wm.open_time and wm.close_time
    key, (wm, batch) = key__wm__batch
    return (
        # Use the correct schema for the key
        {"identifier": key, "name": "topic_key"},
        # Use the correct schema for the value
        {
            "identifier": key,
            "avg": sum(batch) / len(batch),
            "window_open": wm.open_time.isoformat(),
            "window_start": wm.close_time.isoformat(),
        },
    )


flow.map("avg", calc_avg)
flow.inspect(lambda x: print(f"Writing: {x}"))

# The output doesn't allow not raising errors on serialization.
flow.output(
    "schema-output",
    KafkaSink(
        [KAFKA_SERVER],
        "out_topic",
        schema_registry=registry,
        # # For ConfluentSchemaRegistry add:
        # add_config={
        #     "security.protocol": "SASL_SSL",
        #     "sasl.mechanism": "PLAIN",
        #     "sasl.username": CONFLUENT_USERNAME,
        #     "sasl.password": CONFLUENT_PASSWORD,
        # },
    ),
)
