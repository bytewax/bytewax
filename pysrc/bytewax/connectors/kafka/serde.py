"""Serializers and deserializers for Kafka messages."""
import io
import json
import logging
from typing import Dict, Optional, Union

from confluent_kafka.schema_registry import Schema
from confluent_kafka.serialization import Deserializer, SerializationContext, Serializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import AvroMessage
from typing_extensions import override

from bytewax.connectors.kafka import MaybeStrBytes

_logger = logging.getLogger(__name__)


class PlainAvroSerializer(Serializer):
    """Unframed Avro serializer. Encodes into raw Avro.

    This is in comparison to
    {py:obj}`confluent_kafka.schema_registry.avro.AvroSerializer`
    which prepends magic bytes to the Avro payload which specify the
    schema ID. This serializer will not prepend those magic bytes. If
    downstream deserializers expect those magic bytes, use
    {py:obj}`~confluent_kafka.schema_registry.avro.AvroSerializer`
    instead.

    """

    def __init__(
        self, schema: Union[str, Schema], named_schemas: Optional[Dict] = None
    ):
        """Init.

        :arg schema: Selected schema to use.

        :arg named_schemas: Other schemas the selected schema
            references. See documentation for
            {py:obj}`fastavro._schema_py.parse_schema`.

        """
        if isinstance(schema, Schema):
            schema_str = schema.schema_str
        else:
            schema_str = schema
        self.schema = parse_schema(json.loads(schema_str), named_schemas=named_schemas)

    @override
    def __call__(self, obj: Dict, ctx: Optional[SerializationContext] = None) -> bytes:
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class PlainAvroDeserializer(Deserializer):
    """Unframed Avro deserializer. Decodes from raw Avro.

    Requires you to manually specify the schema to use.

    This is in comparison to
    {py:obj}`confluent_kafka.schema_registry.avro.AvroDeserializer`
    which expects magic bytes in the output proceeding the actual Avro
    payload. This deserializer _can not_ handle those bytes and will
    throw an exception. If upstream serializers are including magic
    bytes, use
    {py:obj}`~confluent_kafka.schema_registry.avro.AvroDeserializer`
    instead.

    """

    def __init__(
        self, schema: Union[str, Schema], named_schemas: Optional[Dict] = None
    ):
        """Init.

        :arg schema: Selected schema to use.

        :arg named_schemas: Other schemas the selected schema
            references. See documentation for
            {py:obj}`fastavro._schema_py.parse_schema`.

        """
        if isinstance(schema, Schema):
            schema_str = schema.schema_str
        else:
            schema_str = schema
        self.schema = parse_schema(json.loads(schema_str), named_schemas=named_schemas)

    @override
    def __call__(
        self, obj: MaybeStrBytes, ctx: Optional[SerializationContext] = None
    ) -> AvroMessage:
        if obj is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(obj, str):
            obj = obj.encode()
        payload = io.BytesIO(obj)
        return schemaless_reader(payload, self.schema, None)
