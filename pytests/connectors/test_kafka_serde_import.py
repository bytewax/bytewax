"""`bytewax.connectors.kafka.serde` must not require Schema Registry deps.

Since confluent-kafka 2.7, importing `confluent_kafka.schema_registry`
needs the `schemaregistry` extra (`httpx`, `attrs`, ...), which
`bytewax[kafka]` does not install. Plain Avro serde has no use for a
registry, so it must import and work without it.

"""

import importlib
import json
import sys

from pytest import MonkeyPatch

SCHEMA = json.dumps({"type": "string"})


def _import_serde_without_registry(monkeypatch: MonkeyPatch):
    # A `None` entry makes `import confluent_kafka.schema_registry`
    # raise `ImportError`, as it does without the extra installed.
    for name in list(sys.modules):
        if name.startswith("confluent_kafka.schema_registry"):
            monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "confluent_kafka.schema_registry", None)
    monkeypatch.delitem(sys.modules, "bytewax.connectors.kafka.serde", raising=False)
    return importlib.import_module("bytewax.connectors.kafka.serde")


def test_serde_imports_without_schema_registry(monkeypatch):
    serde = _import_serde_without_registry(monkeypatch)

    encoded = serde.PlainAvroSerializer(SCHEMA)("abc")

    assert serde.PlainAvroDeserializer(SCHEMA)(encoded) == "abc"


def test_serde_accepts_schema_like_object_without_registry(monkeypatch):
    serde = _import_serde_without_registry(monkeypatch)

    class FakeSchema:
        schema_str = SCHEMA

    encoded = serde.PlainAvroSerializer(FakeSchema())("abc")

    assert serde.PlainAvroDeserializer(FakeSchema())(encoded) == "abc"
