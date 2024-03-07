"""
Serialization and deserialization using confluent's schema registry
and confluent's wire avro format.

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
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import bytewax.operators as op
import bytewax.operators.window as wop
from bytewax.connectors.kafka import KafkaSinkMessage, KafkaSourceMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

logger = logging.getLogger(__name__)
logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.WARNING)


# Setup some more env vars first:
KAFKA_BROKERS = os.environ.get("KAFKA_SERVER", "localhost:19092").split(";")
IN_TOPICS = os.environ.get("KAFKA_IN_TOPIC", "in_topic").split(";")
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", "out_topic")
CONFLUENT_URL = os.environ["CONFLUENT_URL"]
CONFLUENT_USERINFO = os.environ["CONFLUENT_USERINFO"]
CONFLUENT_USERNAME = os.environ["CONFLUENT_USERNAME"]
CONFLUENT_PASSWORD = os.environ["CONFLUENT_PASSWORD"]

# Pass this to both kafka_in and kafka_out
add_config = {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": CONFLUENT_USERNAME,
    "sasl.password": CONFLUENT_PASSWORD,
}

flow = Dataflow("schema_registry")
kinp = kop.input(
    "kafka-in", flow, brokers=KAFKA_BROKERS, topics=IN_TOPICS, add_config=add_config
)
# Inspect errors and crash
op.inspect("inspect-kafka-errors", kinp.errs).then(op.raises, "kafka-error")

# ConfluentSchemaRegistry config:
client = SchemaRegistryClient(
    {"url": CONFLUENT_URL, "basic.auth.user.info": CONFLUENT_USERINFO}
)

# Confluent's deserializer doesn't need a schema, it automatically fetches it.
key_de = AvroDeserializer(client)
val_de = AvroDeserializer(client)
msgs = kop.deserialize("de", kinp.oks, key_deserializer=key_de, val_deserializer=val_de)
# Inspect errors and crash
op.inspect("inspect-deser", msgs.errs).then(op.raises, "deser-error")


def extract_identifier(msg: KafkaSourceMessage) -> str:
    # Use the "identifier" field of the key as bytewax's key
    return msg.key["identifier"]


keyed = op.key_on("key_on_identifier", msgs.oks, extract_identifier)


# Let's window the input
def accumulate(acc: List[str], msg: KafkaSourceMessage) -> List[str]:
    acc.append(msg.value["value"])
    return acc


cc = SystemClockConfig()
wc = TumblingWindow(timedelta(seconds=1), datetime(2023, 1, 1, tzinfo=timezone.utc))
windows = wop.fold_window("calc_avg", keyed, cc, wc, list, accumulate)


# And do some calculations on each window
def calc_avg(key__wm__batch) -> KafkaSinkMessage[Dict, Dict]:
    key, (wm, batch) = key__wm__batch
    # Use the correct schemas here, or the serialization
    # step will fail later
    return KafkaSinkMessage(
        key={"identifier": key, "name": "topic_key"},
        value={
            "identifier": key,
            "avg": sum(batch) / len(batch),
            "window_start": wm.open_time.isoformat(),
            "window_end": wm.close_time.isoformat(),
        },
    )


avgs = op.map("avg", windows, calc_avg)
op.inspect("inspect-out-data", avgs)

# The serializers require a specific schema instead. Get it from the client.
key_schema = client.get_schema(100002)
val_schema = client.get_schema(100003)
key_ser = AvroSerializer(client, key_schema.schema_str)
val_ser = AvroSerializer(client, val_schema.schema_str)
# Serialize
serialized = kop.serialize("ser", avgs, key_serializer=key_ser, val_serializer=val_ser)

op.inspect("inspect-serialized", serialized)
kop.output(
    "kafka-out",
    serialized,
    brokers=KAFKA_BROKERS,
    topic=OUT_TOPIC,
    add_config=add_config,
)
