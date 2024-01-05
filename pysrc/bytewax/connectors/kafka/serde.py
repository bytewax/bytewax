"""Serializers and deserializers for kafka messages."""
import io
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Generic, TypeVar

from confluent_kafka.schema_registry import record_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import AvroMessage

from ._types import MaybeStrBytes

__all__ = [
    "SerdeIn",
    "SerdeOut",
    "SchemaSerializer",
    "SchemaDeserializer",
    "AvroMessage",
]

logger = logging.getLogger(__name__)

SerdeOut = TypeVar("SerdeOut")
SerdeIn = TypeVar("SerdeIn")


class SchemaSerializer(ABC, Generic[SerdeIn, SerdeOut]):
    """A serializer for a specific schema."""

    @abstractmethod
    def ser(self, obj: SerdeIn) -> SerdeOut:
        """Serialize an object."""
        ...


class SchemaDeserializer(ABC, Generic[SerdeIn, SerdeOut]):
    """A deserializer for a specific schema."""

    @abstractmethod
    def de(self, data: SerdeIn) -> SerdeOut:
        """Deserialize data."""
        ...


class _ConfluentAvroSerializer(SchemaSerializer[Dict, bytes]):
    def __init__(self, client, schema_str):
        # Use a different "subject.name.strategy" than the default. See:
        # https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy
        self.serializer = AvroSerializer(
            client,
            schema_str,
            conf={"subject.name.strategy": record_subject_name_strategy},
        )

    def ser(self, obj: Dict) -> bytes:
        return self.serializer(obj, ctx=None)


class _ConfluentAvroDeserializer(SchemaDeserializer[MaybeStrBytes, AvroMessage]):
    def __init__(self, client):
        self.deserializer = AvroDeserializer(client)

    def de(self, data: MaybeStrBytes) -> Dict:
        if data is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(data, str):
            data = data.encode()
        # Here the ctx is only used if a custom `from_dict`
        # function is passed to the deserializer, but we
        # initialize it ourselves and don't pass that,
        # so we can set `ctx` to None
        return self.deserializer(data, ctx=None)


class _AvroSerializer(SchemaSerializer[Dict, bytes]):
    def __init__(self, schema):
        self.schema = parse_schema(json.loads(schema))

    def ser(self, obj: Dict) -> bytes:
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class _AvroDeserializer(SchemaDeserializer[MaybeStrBytes, AvroMessage]):
    def __init__(self, schema):
        self.schema = parse_schema(json.loads(schema))

    def de(self, data: MaybeStrBytes) -> AvroMessage:
        if data is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(data, str):
            data = data.encode()
        payload = io.BytesIO(data)
        return schemaless_reader(payload, self.schema, None)
