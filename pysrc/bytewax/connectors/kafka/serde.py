"""TODO."""
import io
import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
)
from fastavro import parse_schema, schemaless_reader, schemaless_writer

logger = logging.getLogger(__name__)


class SchemaSerializer(ABC):
    """TODO."""

    @abstractmethod
    def ser(self, obj: dict, topic: str) -> bytes:
        """TODO."""
        ...


class SchemaDeserializer(ABC):
    """TODO."""

    @abstractmethod
    def de(self, data: bytes, topic: str) -> dict:
        """TODO."""
        ...


class _ConfluentAvroSerializer(SchemaSerializer):
    def __init__(self, client, schema_str, is_key):
        self.serializer = AvroSerializer(client, schema_str)
        self.is_key = is_key

    def ser(self, obj, topic):
        if self.is_key:
            ctx = SerializationContext(topic, MessageField.KEY)
        else:
            ctx = SerializationContext(topic, MessageField.VALUE)
        try:
            return self.serializer(obj, ctx)
        except SerializationError as e:
            logger.error(f"Error serializing data: {obj}")
            raise e


class _ConfluentAvroDeserializer(SchemaDeserializer):
    def __init__(self, client, is_key):
        self.deserializer = AvroDeserializer(client)
        self.field = MessageField.KEY if is_key else MessageField.VALUE

    def de(self, data, topic):
        ctx = SerializationContext(topic, self.field)
        try:
            return self.deserializer(data, ctx)
        except SerializationError as e:
            logger.error(f"Error deserializing data: {data}")
            raise e


class _AvroSerializer(SchemaSerializer):
    def __init__(self, schema, is_key):
        # TODO: Not sure why I need to json.loads the schema,
        # the function should accept a str too, but it raises
        # UnkownType error if I do
        self.schema = parse_schema(json.loads(schema))

    def ser(self, obj, topic):
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class _AvroDeserializer(SchemaDeserializer):
    def __init__(self, schema, is_key):
        # TODO: Not sure why I need to json.loads the schema,
        # the function should accept a str too, but it raises
        # UnkownType error if I do
        self.schema = parse_schema(json.loads(schema))

    def de(self, msg, _topic):
        payload = io.BytesIO(msg)
        return schemaless_reader(payload, self.schema)
