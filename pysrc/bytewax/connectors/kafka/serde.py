"""Serializers and deserializers for kafka messages."""
import io
import json
import logging
from typing import Dict, Optional

from confluent_kafka.serialization import SerializationContext
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from fastavro.types import AvroMessage

from bytewax.connectors.kafka import MaybeStrBytes

_logger = logging.getLogger(__name__)


class PlainAvroSerializer:
    """Bytewax serializer that uses fastavro's serializer.

    Beware that using plain avro serializers means you can't
    deserialize with `confluent_kafka` python library, as that
    requires some metadata prepended to each message, and plain
    avro doesn't do that.
    If you plan to deserialize messages with `confluent_kafka` python
    library, please use the `ConfluentAvroSerializer` instead.
    """

    def __init__(self, schema_str: str):
        """Initialize an avro serializer."""
        self.schema = parse_schema(json.loads(schema_str))

    def __call__(self, obj: Dict, ctx: Optional[SerializationContext] = None) -> bytes:
        """TODO."""
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, self.schema, obj)
        return bytes_writer.getvalue()


class PlainAvroDeserializer:
    """Bytewax deserializer that uses fastavro's deserializer.

    Beware that this can't deserialize messages serialized with
    `confluent_kafka` python library, as that prepends some magic bytes
    and the schema_id to each message, and plain avro doesn't do that.
    If you want to deserialize messages serialized with `confluent_kafka`
    python library, please use the `ConfluentAvroDeserializer` instead.
    """

    def __init__(self, schema_str: str):
        """Initialize an avro deserializer."""
        self.schema = parse_schema(json.loads(schema_str))

    def __call__(
        self, data: MaybeStrBytes, ctx: Optional[SerializationContext] = None
    ) -> AvroMessage:
        """TODO."""
        if data is None:
            msg = "Can't deserialize None data"
            raise ValueError(msg)
        if isinstance(data, str):
            data = data.encode()
        payload = io.BytesIO(data)
        return schemaless_reader(payload, self.schema, None)
