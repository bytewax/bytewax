"""Serializers and deserializers for kafka messages."""
import io
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Generic, TypeVar, cast

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import AvroMessage
from typing_extensions import override

from bytewax.connectors.kafka import MaybeStrBytes

_logger = logging.getLogger(__name__)

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


class ConfluentAvroSerializer(SchemaSerializer[Dict, bytes]):
    """Bytewax serializer that uses confluent_kafka's Avro serializer.

    confluent_kafka's serializer will prepend some magic bytes and the
    `schema_id` to each serialized message so that the deserializer can read
    those bytes and know the right schema to use.

    This is not standard avro format though, so you MUST use confluent_kafka
    deserializers on the other side, or the deserialization will fail.
    """

    def __init__(self, client: SchemaRegistryClient, schema_str: str):
        """Instantiate a ConfluentAvroSerializer."""
        # Use a different "subject.name.strategy" than the default. See:
        # https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#subject-name-strategy
        self.serializer = AvroSerializer(
            client,
            schema_str,
            conf={"subject.name.strategy": record_subject_name_strategy},
        )

    @override
    def ser(self, obj: Dict) -> bytes:
        if not isinstance(obj, Dict):
            msg = f"Serializer needs a dict as input, received: {obj}"
            raise TypeError(msg)

        # Here we can assume the output will be bytes (or fail), since we made
        # sure to pass a dict to the serializer.
        # Also, we can pass `None` as the ctx since our "subject.name.strategy"
        # does not use it, the types are not properly specified in confluent_kafka.
        return cast(bytes, self.serializer(obj, ctx=None))


class ConfluentAvroDeserializer(SchemaDeserializer[MaybeStrBytes, AvroMessage]):
    """Bytewax deserializer that uses confluent's kafka_python Avro deserializer.

    confluent_kafka's deserializer needs some magic bytes and the schema_id to be
    prepended to each serialized message so that it can fetch the right schema to use.

    This is not standard avro format though, so this MUST receive messages serialized
    with confluent_kafka serializer on the other side, or the it will fail.
    """

    def __init__(self, client: SchemaRegistryClient):
        """Instantiate a ConfluentAvroDeserializer."""
        self.deserializer = AvroDeserializer(client)

    @override
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
        return cast(Dict, self.deserializer(data, ctx=None))


class PlainAvroSerializer(SchemaSerializer[Dict, bytes]):
    """Bytewax serializer that uses fastavro's serializer.

    Beware that using plain avro serializers means you can't
    deserialize with `confluent_kafka` python library, as that
    requires some metadata prepended to each message, and plain
    avro doesn't do that.
    If you plan to deserialize messages with `confluent_kafka` python
    library, please use the `ConfluentAvroSerializer` instead.
    """

    def __init__(self, schema):
        """Initialize an avro serializer."""
        self.schema = parse_schema(json.loads(schema))

    @override
    def ser(self, obj: Dict) -> bytes:
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class PlainAvroDeserializer(SchemaDeserializer[MaybeStrBytes, AvroMessage]):
    """Bytewax deserializer that uses fastavro's deserializer.

    Beware that this can't deserialize messages serialized with
    `confluent_kafka` python library, as that prepends some magic bytes
    and the schema_id to each message, and plain avro doesn't do that.
    If you want to deserialize messages serialized with `confluent_kafka`
    python library, please use the `ConfluentAvroDeserializer` instead.
    """

    def __init__(self, schema):
        """Initialize an avro deserializer."""
        self.schema = parse_schema(json.loads(schema))

    @override
    def de(self, data: MaybeStrBytes) -> AvroMessage:
        if data is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(data, str):
            data = data.encode()
        payload = io.BytesIO(data)
        return schemaless_reader(payload, self.schema, None)
