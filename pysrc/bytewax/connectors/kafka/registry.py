"""Schema registries connection.

This module offers two preconfigured schema registries:
- ConfluentSchemaRegistry
- RedpandaSchemaRegistry

Subclass "SchemaRegistry" to implement support for your any registry.
"""
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Union

import requests
from confluent_kafka.schema_registry import SchemaRegistryClient
from fastavro.types import AvroMessage

from ._types import MaybeStrBytes
from .serde import (
    SchemaDeserializer,
    SchemaSerializer,
    _AvroDeserializer,
    _AvroSerializer,
    _ConfluentAvroDeserializer,
    _ConfluentAvroSerializer,
)

__all__ = [
    "SchemaRef",
    "ConfluentSchemaRegistry",
    "RedpandaSchemaRegistry",
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaRef:
    """Info used to retrieve a schema from a schema registry.

    Specify the `subject` and optionally `version`.
    If no `version` is specified, it defaults to the latest schema.
    """

    # TODO: Only `avro` supported for now, but we might want
    #       to add `protobuf` and others too
    # format: str = "avro"
    subject: str
    version: Optional[int] = None


class ConfluentSchemaRegistry:
    """Confluent's schema registry for Kafka's input and output connectors."""

    def __init__(self, client: SchemaRegistryClient):
        """Init.

        Args:
            client:
                Configured `confluent_kafka.schema_registry.SchemaRegistryClient`
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
        self, schema_ref: Union[int, SchemaRef]
    ) -> SchemaSerializer[Dict, bytes]:
        """Confluent avro serializer.

        Specify either the `schema_id` or a `SchemaRef` instance.
        """
        schema = self._get_schema(schema_ref)
        return _ConfluentAvroSerializer(self.client, schema)

    def deserializer(self) -> SchemaDeserializer[MaybeStrBytes, AvroMessage]:
        """Confluent avro deserializer.

        `schema_ref` is not needed since Confluent cloud adds the schema_id
        as metadata in each message.
        The client will automatically fetch and cache the schemas needed.
        """
        return _ConfluentAvroDeserializer(self.client)


class RedpandaSchemaRegistry:
    """Redpanda's schema registry client."""

    def __init__(self, base_url: str = "http://localhost:8081"):
        """Init.

        Args:
            base_url:
                Base url of redpanda's schema registry instance
        """
        self._base_url = base_url

    def _get_schema(self, schema_ref: Union[int, SchemaRef]) -> bytes:
        # Schema can bew retrieved by `schema_id`, or by
        # specifying a `subject` and a `version`, which
        # defaults to `latest`.
        if isinstance(schema_ref, int):
            url = f"{self._base_url}/schemas/{schema_ref}/schema"
        else:
            version = schema_ref.version or "latest"
            url = (
                f"{self._base_url}/subjects/"
                f"{schema_ref.subject}/versions/"
                f"{version}/schema"
            )
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content

    def serializer(
        self, schema_ref: Union[int, SchemaRef]
    ) -> SchemaSerializer[Dict, bytes]:
        """Fastavro serializer.

        Specify either the `schema_id` or a `SchemaRef` instance.
        """
        schema = self._get_schema(schema_ref)
        return _AvroSerializer(schema)

    def deserializer(
        self, schema_ref: Union[int, SchemaRef]
    ) -> SchemaDeserializer[MaybeStrBytes, AvroMessage]:
        """Fastavro deserializer.

        Specify either the `schema_id` or a `SchemaRef` instance.
        """
        schema = self._get_schema(schema_ref)
        return _AvroDeserializer(schema)
