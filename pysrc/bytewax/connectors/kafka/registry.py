"""TODO."""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

import avro.schema
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

    Either use `schema_id`, or retrieve a schema by specifying
    the `subject` and optionally `version`.
    If no `version` is specified, defaults to `latest`
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
    def key_serializer(self, *args, **kwargs) -> SchemaSerializer:
        """TODO."""
        ...

    @abstractmethod
    def value_serializer(self, *args, **kwargs) -> SchemaSerializer:
        """TODO."""
        ...

    @abstractmethod
    def key_deserializer(self, *args, **kwargs) -> SchemaDeserializer:
        """TODO."""
        ...

    @abstractmethod
    def value_deserializer(self, *args, **kwargs) -> SchemaDeserializer:
        """TODO."""
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
        value_conf: Optional[SchemaConf],
        key_conf: Optional[SchemaConf],
    ):
        """Init.

        Args:
            sr_conf:
                Configuration for `confluent_kafka.schema_registry.SchemaRegistryClient`
            value_conf:
                SchemaConf used for serialization of values in the output.
            key_conf:
                SchemaConf used for serialization of keys in the output.
        """
        self._sr_conf = sr_conf
        self._value_conf = value_conf
        self._key_conf = key_conf

    def key_serializer(self):
        """See ABC docstring."""
        if self._key_conf is None:
            return None
        client = SchemaRegistryClient(self._sr_conf)
        schema_str = self._get_schema_str(client, self._key_conf)
        return _ConfluentAvroSerializer(client, schema_str, is_key=True)

    def value_serializer(self):
        """See ABC docstring."""
        if self._value_conf is None:
            return None
        client = SchemaRegistryClient(self._sr_conf)
        schema_str = self._get_schema_str(client, self._value_conf)
        return _ConfluentAvroSerializer(client, schema_str, is_key=False)

    def key_deserializer(self):
        """See ABC docstring."""
        client = SchemaRegistryClient(self._sr_conf)
        return _ConfluentAvroDeserializer(client, is_key=True)

    def value_deserializer(self):
        """See ABC docstring."""
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
    """TODO."""

    def __init__(
        self,
        base_url: str = "http://localhost:18081",
        input_value_conf: Optional[SchemaConf] = None,
        input_key_conf: Optional[SchemaConf] = None,
        output_value_conf: Optional[SchemaConf] = None,
        output_key_conf: Optional[SchemaConf] = None,
    ):
        """TODO."""
        self._base_url = base_url
        self._input_key_schema = self._get_schema_str(input_key_conf)
        self._input_value_schema = self._get_schema_str(input_value_conf)
        self._output_key_schema = self._get_schema_str(output_key_conf)
        self._output_value_schema = self._get_schema_str(output_value_conf)

    def key_serializer(self, *args, **kwargs):
        """See ABC docstring."""
        return _AvroSerializer(self._output_key_schema, is_key=True)

    def value_serializer(self, *args, **kwargs):
        """See ABC docstring."""
        return _AvroSerializer(self._output_value_schema, is_key=False)

    def key_deserializer(self, *args, **kwargs):
        """See ABC docstring."""
        return _AvroDeserializer(self._input_key_schema, is_key=True)

    def value_deserializer(self, *args, **kwargs):
        """See ABC docstring."""
        return _AvroDeserializer(self._input_value_schema, is_key=False)

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
        schema_content = requests.get(url).content
        return avro.schema.parse(schema_content)
