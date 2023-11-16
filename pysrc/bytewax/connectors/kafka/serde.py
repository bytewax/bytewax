"""TODO."""
import io
import logging
from abc import ABC, abstractmethod

from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
)

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
        self.schema = schema

    def ser(self, obj, topic):
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer = DatumWriter(self.schema)
        writer.write(obj, encoder)
        return bytes_writer.getvalue()


class _AvroDeserializer(SchemaDeserializer):
    def __init__(self, schema, is_key):
        self.reader = DatumReader(schema)

    def de(self, msg, _topic):
        message_bytes = io.BytesIO(msg)
        decoder = BinaryDecoder(message_bytes)
        return self.reader.read(decoder)
