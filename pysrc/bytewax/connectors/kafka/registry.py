"""Schema registry connection.

This module offers the ability to instantiate (de)serializers
reading schemas from any registry that is api compatible with
Confluent's schema registry (like Redpanda's schema registry).

See {py:obj}`~bytewax.connectors.kafka.registry.SchemaRegistry`.
It requires a fully configured instance of a
{py:obj}`confluent_kafka.schema_registry.SchemaRegistryClient`
to work.

"""
import logging
from dataclasses import dataclass
from typing import Dict, Literal, Optional, Union

from confluent_kafka.schema_registry import SchemaRegistryClient
from fastavro.types import AvroMessage

from bytewax.connectors.kafka import MaybeStrBytes
from bytewax.connectors.kafka.serde import (
    ConfluentAvroDeserializer,
    ConfluentAvroSerializer,
    PlainAvroDeserializer,
    PlainAvroSerializer,
    SchemaDeserializer,
    SchemaSerializer,
)

_logger = logging.getLogger(__name__)

# Supported (de)serialization formats.
SerdeFormats = Literal["plain-avro", "confluent-avro"]


@dataclass(frozen=True)
class SchemaRef:
    """Info used to retrieve a schema from a schema registry."""

    subject: str
    """If not specified, defaults to latest schema."""
    version: Optional[int] = None


class SchemaRegistry:
    """Confluent's schema registry for Kafka's input and output connectors.

    Compatible with Redpanda's schema registry.
    """

    def __init__(self, client: SchemaRegistryClient):
        """Init.

        :arg client: Configured
            {py:obj}`confluent_kafka.schema_registry.SchemaRegistryClient`.

        """
        self.client = client

    def _get_schema(self, schema_ref: Union[int, SchemaRef]) -> str:
        # Schema can bew retrieved by `schema_id`, or by
        # specifying a `subject` and a `version`, which
        # defaults to `latest`.
        if isinstance(schema_ref, int):
            schema = self.client.get_schema(schema_ref)
        else:
            subject = schema_ref.subject
            version = schema_ref.version
            if version is None:
                schema = self.client.get_latest_version(subject).schema
            else:
                schema = self.client.get_version(subject, version).schema
        return schema.schema_str

    def serializer(
        self,
        schema_ref: Union[int, SchemaRef],
        serde_format: SerdeFormats = "confluent-avro",
    ) -> SchemaSerializer[Dict, bytes]:
        """Returns a serializer.

        Specify either the `schema_id` or a `SchemaRef` instance.
        By default `confluent-avro` format is assumed.
        See module's docstrings for more details about that.
        """
        schema = self._get_schema(schema_ref)
        if serde_format == "confluent-avro":
            return ConfluentAvroSerializer(self.client, schema)
        elif serde_format == "plain-avro":
            return PlainAvroSerializer(schema)
        else:
            msg = f"Unsupported serde_format: {serde_format}"
            raise ValueError(msg)

    def deserializer(
        self,
        schema_ref: Optional[Union[int, SchemaRef]] = None,
        serde_format: SerdeFormats = "confluent-avro",
    ) -> SchemaDeserializer[MaybeStrBytes, AvroMessage]:
        """Confluent avro deserializer.

        `schema_ref` is optional if you choose the default "confluent-avro"
        serde_format, since Confluent's serialization library adds the schema_id as
        metadata in each message. The client will automatically fetch and cache the
        schemas needed.

        If you chose "plain-avro", you instead need to define a single schema that will
        be used for each message.
        """
        if serde_format == "confluent-avro":
            if schema_ref is not None:
                _logger.warning("schema_ref supplied to deserialized will be ignored")
            return ConfluentAvroDeserializer(self.client)
        elif serde_format == "plain-avro":
            assert (
                schema_ref is not None
            ), "schema_ref required for 'plain-avro' deserialization format"
            schema = self._get_schema(schema_ref)
            return PlainAvroDeserializer(schema)
        else:
            msg = f"Unsupported serde_format: {serde_format}"
            raise ValueError(msg)
