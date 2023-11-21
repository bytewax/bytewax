"""TODO."""
import io
import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)
from fastavro import parse_schema, schemaless_reader, schemaless_writer

logger = logging.getLogger(__name__)


class SerializationError(ValueError):
    """(de)serialization exception."""


class SchemaSerializer(ABC):
    """A serializer for a specific schema."""

    @abstractmethod
    def ser(self, obj: dict, topic: str) -> bytes:
        """Serialize data.

        The `topic` argument can be ignored if not needed.

        Do not catch serialization errors, let the source handle it.
        """
        ...


class SchemaDeserializer(ABC):
    """A deserializer for a specific schema."""

    @abstractmethod
    def de(self, data: bytes, topic: str) -> dict:
        """Deserialize data.

        The `topic` argument can be ignored if not needed.

        Do not catch deserialization errors, let the source handle it.

        """
        ...


class _ConfluentAvroSerializer(SchemaSerializer):
    def __init__(self, client, schema_str, is_key):
        self.serializer = AvroSerializer(client, schema_str)
        self.ctx = SerializationContext(
            # This value will be updated at each message
            "PLACEHOLDER",
            MessageField.KEY if is_key else MessageField.VALUE,
        )

    def ser(self, obj, topic):
        self.ctx.topic = topic
        return self.serializer(obj, self.ctx)


class _ConfluentAvroDeserializer(SchemaDeserializer):
    def __init__(self, client, is_key):
        self.deserializer = AvroDeserializer(client)
        self.ctx = SerializationContext(
            # This value will be updated at each message
            "PLACEHOLDER",
            MessageField.KEY if is_key else MessageField.VALUE,
        )

    def de(self, data, topic):
        self.ctx.topic = topic
        return self.deserializer(data, self.ctx)


class _AvroSerializer(SchemaSerializer):
    def __init__(self, schema):
        # TODO: Not sure why I need to json.loads the schema,
        # the function should accept a str too, but it raises
        # UnkownType error if I do
        self.schema = parse_schema(json.loads(schema))

    def ser(self, obj, topic):
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class _AvroDeserializer(SchemaDeserializer):
    def __init__(self, schema):
        # TODO: Not sure why I need to json.loads the schema,
        # the function should accept a str too, but it raises
        # UnkownType error if I do
        self.schema = parse_schema(json.loads(schema))

    def de(self, data, _topic):
        payload = io.BytesIO(data)
        return schemaless_reader(payload, self.schema)
