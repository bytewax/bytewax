"""
Serialization and deserialization using redpanda's schema registry
and plain avro format.

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
from bytewax.connectors.kafka.serde import PlainAvroDeserializer, PlainAvroSerializer
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow
from confluent_kafka.schema_registry import SchemaRegistryClient

logger = logging.getLogger(__name__)
logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.WARNING)

KAFKA_BROKERS = os.environ.get("KAFKA_SERVER", "localhost:19092").split(";")
IN_TOPICS = os.environ.get("KAFKA_IN_TOPIC", "in-topic").split(";")
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", "out_topic")
REDPANDA_REGISTRY_URL = os.environ["REDPANDA_REGISTRY_URL"]


flow = Dataflow("schema_registry")
kinp = kop.input("kafka-in", flow, brokers=KAFKA_BROKERS, topics=IN_TOPICS)

# Inspect errors and crash
op.inspect("inspect-kafka-errors", kinp.errs).then(op.raises, "kafka-error")

# Redpanda's schema registry configuration
client = SchemaRegistryClient({"url": REDPANDA_REGISTRY_URL})

# Use plain avro instead of confluent's wire format.
# We need to specify the schema in the deserializer too here.
key_schema = client.get_latest_version("sensor-key").schema
key_de = PlainAvroDeserializer(schema_str=key_schema.schema_str)

val_schema = client.get_latest_version("sensor-value").schema
val_de = PlainAvroDeserializer(schema_str=val_schema.schema_str)

# Deserialize both key and value
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

key_ser = PlainAvroSerializer(schema_str=key_schema.schema_str)
out_val_schema = client.get_latest_version("aggregated-value").schema
val_ser = PlainAvroSerializer(schema_str=out_val_schema.schema_str)

# Serialize
serialized = kop.serialize("ser", avgs, key_serializer=key_ser, val_serializer=val_ser)

op.inspect("inspect-serialized", serialized)
kop.output("kafka-out", serialized, brokers=KAFKA_BROKERS, topic=OUT_TOPIC)
