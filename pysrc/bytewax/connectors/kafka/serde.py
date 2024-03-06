"""Serializers and deserializers for kafka messages."""
import io
import json
import logging
from typing import Dict, Optional, Union

from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import Deserializer, SerializationContext, Serializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import AvroMessage

from bytewax.connectors.kafka import MaybeStrBytes

_logger = logging.getLogger(__name__)


class PlainAvroSerializer(Serializer):
    """Unframed Avro serializer. Encodes into raw Avro.

    This is in comparison to {py:obj}`confluent_kafka.serialization.AvroSerializer`
    which prepends magic bytes to the Avro payload which specify the schema ID. This
    serializer will not prepend those magic bytes. If downstream deserializers expect
    those  magic bytes, use {py:obj}`~confluent_kafka.serialization.AvroSerializer`
    instead.
    """

    def __init__(
        self, schema: Union[str, Schema], named_schemas: Optional[Dict] = None
    ):
        """Initialize an avro serializer.

        See documentation for fastavro's {py:obj}`~fastavro.parse_schema` for how to use
        the `named_schemas` argument to reference other schemas by name when parsing.
        """
        if isinstance(schema, Schema):
            schema_str = schema.schema_str
        else:
            schema_str = schema
        self.schema = parse_schema(json.loads(schema_str), named_schemas=named_schemas)

    def __call__(self, obj: Dict, ctx: Optional[SerializationContext] = None) -> bytes:
        """Call this to serialize an object."""
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class PlainAvroDeserializer(Deserializer):
    """Unframed Avro deserializer. Decodes from raw Avro.

    Requires you to manually specify the schema to use.

    This is in comparison to {py:obj}`confluent_kafka.serialization.AvroDeserializer`
    which expects magic bytes in the output proceeding the actual Avro
    payload. This deserializer _can not_ handle those bytes and will throw an
    exception. If upstream serializers are including magic bytes, use {py:obj}
    `~confluent_kafka.serialization.AvroDeserializer` instead.
    """

    def __init__(
        self, schema: Union[str, Schema], named_schemas: Optional[Dict] = None
    ):
        """Initialize an avro deserializer."""
        if isinstance(schema, Schema):
            schema_str = schema.schema_str
        else:
            schema_str = schema
        self.schema = parse_schema(json.loads(schema_str), named_schemas=named_schemas)

    def __call__(
        self, data: MaybeStrBytes, ctx: Optional[SerializationContext] = None
    ) -> AvroMessage:
        """Call this to deserialize data."""
        if data is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(data, str):
            data = data.encode()
        payload = io.BytesIO(data)
        return schemaless_reader(payload, self.schema, None)
