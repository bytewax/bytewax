"""
Schema registry complete example.

The Kafka input connector has support for schema registries.
We support Redpanda and Confluent registries.
This example shows how to use Redpanda client in the kafka connector.

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
from bytewax.connectors.kafka.registry import SchemaRef, SchemaRegistry
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow
from confluent_kafka.schema_registry import SchemaRegistryClient

logger = logging.getLogger(__name__)
logging.basicConfig(format=logging.BASIC_FORMAT, level=logging.WARNING)

KAFKA_BROKERS = os.environ.get("KAFKA_SERVER", "localhost:19092").split(";")
IN_TOPICS = os.environ.get("KAFKA_IN_TOPIC", "in_topic").split(";")
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", "out_topic")
REDPANDA_REGISTRY_URL = os.environ["REDPANDA_REGISTRY_URL"]


flow = Dataflow("schema_registry")
kinp = kop.input("kafka-in", flow, brokers=KAFKA_BROKERS, topics=IN_TOPICS)

# Inspect errors and crash
op.inspect("inspect-kafka-errors", kinp.errs).then(op.raises, "kafka-error")

# Redpanda's schema registry configuration
sr_conf = {"url": REDPANDA_REGISTRY_URL}
registry = SchemaRegistry(SchemaRegistryClient(sr_conf))

# Deserialize both key and value
key_de = registry.deserializer(SchemaRef("sensor-key"), serde_format="plain-avro")
val_de = registry.deserializer(SchemaRef("sensor-value"), serde_format="plain-avro")
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
            "window_open": wm.open_time.isoformat(),
            "window_start": wm.close_time.isoformat(),
        },
    )


avgs = op.map("avg", windows, calc_avg)

op.inspect("inspect-out-data", avgs)

# Instantiate deserializers
key_ser = registry.serializer(SchemaRef("sensor-key"))
val_ser = registry.serializer(SchemaRef("aggregated-value"))
# Serialize
serialized = kop.serialize("ser", avgs, key_serializer=key_ser, val_serializer=val_ser)

op.inspect("inspect-serialized", serialized)
kop.output("kafka-out", serialized, brokers=KAFKA_BROKERS, topic=OUT_TOPIC)
