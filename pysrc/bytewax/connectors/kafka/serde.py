"""Serializers and deserializers for Kafka messages."""

import io
import json
import logging
from typing import TYPE_CHECKING, Dict, Optional, Union

from confluent_kafka.serialization import Deserializer, SerializationContext, Serializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer

if TYPE_CHECKING:
    # Only for type hints: importing `confluent_kafka.schema_registry`
    # at runtime needs the `confluent-kafka[schemaregistry]` extra
    # (since confluent-kafka 2.7), which plain Avro serde doesn't use.
    from confluent_kafka.schema_registry import Schema

_logger = logging.getLogger(__name__)


def _schema_str(schema: Union[str, "Schema"]) -> str:
    if isinstance(schema, str):
        return schema
    return schema.schema_str


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
        self, schema: Union[str, "Schema"], named_schemas: Optional[Dict] = None
    ):
        """Init.

        :arg schema: Selected schema to use.

        :arg named_schemas: Other schemas the selected schema
            references. See documentation for
            {py:obj}`fastavro._schema_py.parse_schema`.

        """
        self.schema = parse_schema(
            json.loads(_schema_str(schema)), named_schemas=named_schemas
        )

    # TODO: Re-enable once we get type hints for `confluent_kafka`.
    # @override
    def __call__(  # noqa: D102
        self,
        obj: Optional[object],
        ctx: Optional[SerializationContext] = None,
    ) -> Optional[bytes]:
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
        self, schema: Union[str, "Schema"], named_schemas: Optional[Dict] = None
    ):
        """Init.

        :arg schema: Selected schema to use.

        :arg named_schemas: Other schemas the selected schema
            references. See documentation for
            {py:obj}`fastavro._schema_py.parse_schema`.

        """
        self.schema = parse_schema(
            json.loads(_schema_str(schema)), named_schemas=named_schemas
        )

    # TODO: Re-enable once we get type hints for `confluent_kafka`.
    # @override
    def __call__(  # noqa: D102
        self,
        value: Optional[bytes],
        ctx: Optional[SerializationContext] = None,
    ) -> Optional[object]:
        if value is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(value, str):
            value = value.encode()
        payload = io.BytesIO(value)
        return schemaless_reader(payload, self.schema, None)
