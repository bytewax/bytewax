"""TODO."""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

import requests
from confluent_kafka.schema_registry import SchemaRegistryClient

from .serde import (
    SchemaDeserializer,
    SchemaSerializer,
    _AvroDeserializer,
    _AvroSerializer,
    _ConfluentAvroDeserializer,
    _ConfluentAvroSerializer,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaConf:
    """Info used to retrieve a schema from a schema registry.

    Either use `schema_id`, or specify the `subject` and optionally `version`.
    If no `version` is specified, it defaults to the latest schema.
    """

    # TODO: Only `avro` supported for now, but we might want
    #       to add `protobuf` and others too
    # format: str = "avro"
    schema_id: Optional[int] = None
    subject: Optional[str] = None
    version: Optional[int] = None

    def __post_init__(self):
        """Validate the data."""
        if self.schema_id is None:
            if self.subject is None:
                msg = "subject MUST be specified if no schema_id is provided"
                raise ValueError(msg)
        else:
            if self.subject is not None:
                logger.warning(
                    "both schema_id and subject specified, subject will be ignored"
                )


class SchemaRegistry(ABC):
    """An interface to define a SchemaRegistry.

    A schema registry must define functions to build
    a (de)serializer for both key and value of a KafkaMessage.
    """

    @abstractmethod
    def key_serializer(self) -> SchemaSerializer:
        """Return a serializer for keys."""
        ...

    @abstractmethod
    def value_serializer(self) -> SchemaSerializer:
        """Return a serializer for values."""
        ...

    @abstractmethod
    def key_deserializer(self) -> SchemaDeserializer:
        """Return a deserializer for keys."""
        ...

    @abstractmethod
    def value_deserializer(self) -> SchemaDeserializer:
        """Return a deserializer for values."""
        ...


class ConfluentSchemaRegistry(SchemaRegistry):
    """Confluent's schema registry for Kafka's input and output connectors.

    Serializer: Use either `schema_id` or `subject`+`version` to fetch
    the schema for the serializer from the registry.

    Deserializer: The deserializer automatically fetches the correct
    schema for each message, following confluent_kafka's behaviour
    """

    def __init__(
        self,
        sr_conf: Dict,
        key_serialization_schema: Optional[SchemaConf] = None,
        value_serialization_schema: Optional[SchemaConf] = None,
        enable_key_deserialization: bool = True,
        enable_value_deserialization: bool = True,
    ):
        """Init.

        Args:
            sr_conf:
                Configuration for `confluent_kafka.schema_registry.SchemaRegistryClient`
            value_serialization_schema:
                Optional SchemaConf to serialize values.
            key_serialization_schema:
                Optional SchemaConf to serialie keys.
            enable_key_deserialization:
                Defaults to True. Set this to False if you want to disable
                automatic keys deserialization
            enable_value_deserialization:
                Defaults to True. Set this to False if you want to disable
                automatic value deserialization
        """
        self._sr_conf = sr_conf
        self._value_ser_schema = value_serialization_schema
        self._key_ser_schema = key_serialization_schema
        self._enable_key_deserialization = enable_key_deserialization
        self._enable_value_deserialization = enable_value_deserialization

    def key_serializer(self):
        """Confluent avro serializer for key_serialization_schema."""
        if self._key_ser_schema is None:
            return None

        client = SchemaRegistryClient(self._sr_conf)
        schema_str = self._get_schema_str(client, self._key_ser_schema)
        return _ConfluentAvroSerializer(client, schema_str, is_key=True)

    def value_serializer(self):
        """Confluent avro serializer for value_serialization_schema."""
        if self._value_ser_schema is None:
            return None

        client = SchemaRegistryClient(self._sr_conf)
        schema_str = self._get_schema_str(client, self._value_ser_schema)
        return _ConfluentAvroSerializer(client, schema_str, is_key=False)

    def key_deserializer(self):
        """Confluent avro deserializer for keys.

        This automatically fetches the schema specified in each message.
        """
        if not self._enable_key_deserialization:
            return None

        client = SchemaRegistryClient(self._sr_conf)
        return _ConfluentAvroDeserializer(client, is_key=True)

    def value_deserializer(self):
        """Confluent avro deserializer for values.

        This automatically fetches the schema specified in each message.
        """
        if not self._enable_value_deserialization:
            return None

        client = SchemaRegistryClient(self._sr_conf)
        return _ConfluentAvroDeserializer(client, is_key=False)

    @staticmethod
    def _get_schema_str(client, schema_conf) -> str:
        # Schema can bew retrieved by `schema_id`, or by
        # specifying a `subject` and a `version`, which
        # defaults to `latest`.
        if schema_conf.schema_id is not None:
            schema = client.get_schema(schema_conf.schema_id)
        else:
            # If schema_conf.version is None, `get_version`
            # defaults to `latest` here
            schema = client.get_version(schema_conf.subject, schema_conf.version).schema
        return schema.schema_str


class RedpandaSchemaRegistry(SchemaRegistry):
    """Redpanda's schema registry client.

    All schemas are optional, (de)serialization is enabled
    only if a SchemaConf is passed.

    >>> registry = RedpandaSchemaRegistry(
    ...     key_serialization_schema=SchemaConf("key-schema"),
    ...     value_serialization_schema=SchemaConf(schema_id=1),
    ... )
    """

    def __init__(
        self,
        base_url: str = "http://localhost:18081",
        key_deserialization_schema: Optional[SchemaConf] = None,
        value_deserialization_schema: Optional[SchemaConf] = None,
        key_serialization_schema: Optional[SchemaConf] = None,
        value_serialization_schema: Optional[SchemaConf] = None,
    ):
        """Specify only the (de)serialization schemas you want to enable."""
        self._base_url = base_url
        self._key_de_schema = self._get_schema_str(key_deserialization_schema)
        self._value_de_schema = self._get_schema_str(value_deserialization_schema)
        self._key_ser_schema = self._get_schema_str(key_serialization_schema)
        self._value_ser_schema = self._get_schema_str(value_serialization_schema)

    def key_serializer(self):
        """Avro serializer for key_serialization_schema."""
        return _AvroSerializer(self._key_ser_schema)

    def value_serializer(self):
        """Avro serializer for value_serialization_schema."""
        return _AvroSerializer(self._value_ser_schema)

    def key_deserializer(self):
        """Avro deserializer for key_deserialization_schema."""
        return _AvroDeserializer(self._key_de_schema)

    def value_deserializer(self):
        """Avro deserializer for value_deserialization_schema."""
        return _AvroDeserializer(self._value_de_schema)

    def _get_schema_str(self, schema_conf) -> Optional[str]:
        if schema_conf is None:
            return None
        if schema_conf.schema_id is not None:
            url = f"{self._base_url}/schemas/{schema_conf.schema_id}/schema"
        elif schema_conf.subject is not None:
            version = schema_conf.version or "latest"
            url = (
                f"{self._base_url}/subjects/"
                f"{schema_conf.subject}/versions/"
                f"{version}/schema"
            )
        return requests.get(url).content
