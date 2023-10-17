import io

import avro.schema
import requests
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow


# def deserialize(data):
#     print(f"Received: {data[1]}")
#     return registry.decode(data[1])


def modify(key_data):
    key, data = key_data
    # breakpoint()
    data["value"] += 1
    return data


# def serialize(data):
#     print(f"Received: {data}")
#     return "ALL", registry.encode(data)


class RedpandaSchemaRegistry:
    def __init__(
        self,
        subject: str,
        base_url: str = "http://localhost:18081",
        schema_content: str = None,
    ):
        self.base_url = base_url
        self.subject = subject
        self.schema_content = (
            schema_content
            or requests.get(
                f"{self.base_url}/subjects/{self.subject}/versions/latest/schema"
            ).content
        )

    def build_part(self):
        part = RedpandaSchemaRegistry(self.subject, self.base_url, self.schema_content)
        part._schema = avro.schema.parse(self.schema_content)
        part._reader = DatumReader(part._schema)
        return part

    def decode(self, msg):
        message_bytes = io.BytesIO(msg)
        decoder = BinaryDecoder(message_bytes)
        event_dict = self._reader.read(decoder)
        return event_dict

    def encode(self, data):
        writer = DatumWriter(self._schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()


registry_builder = RedpandaSchemaRegistry("sensor-value")
flow = Dataflow()
flow.input(
    "redpanda-input",
    KafkaSource(
        ["localhost:19092"],
        topics=["test_topic"],
        registry=registry_builder,
    ),
)
# flow.map("deserialize", deserialize)
flow.map("modify", modify)
# flow.map("serialize", serialize)
# flow.output("redpanda-out", KafkaSink(["localhost:19092"], "test_topic"))
flow.output("stdout", StdOutSink())


registry = registry_builder.build_part()


def create_temperature_events():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": "localhost:19092"})
    event = {"timestamp": 212, "identifier": "test", "value": 123}
    producer.produce("test_topic", registry.encode(event))
    producer.flush()


create_temperature_events()
