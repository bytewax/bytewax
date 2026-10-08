"""Kafka connector tests that do not need a running broker.

The Kafka client classes the connector instantiates are replaced with
small fakes, so these run everywhere, unlike the broker-backed tests in
`test_kafka.py`.

"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import bytewax.connectors.kafka as kafka_mod
import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaError,
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
)
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka.serde import PlainAvroDeserializer, PlainAvroSerializer
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.testing import TestingSink, TestingSource, run_main
from confluent_kafka import OFFSET_BEGINNING, TopicPartition
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from prometheus_client import REGISTRY
from pytest import fixture, importorskip, raises

TOPIC = "topic-a"

AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "Reading",
        "fields": [
            {"name": "sensor", "type": "string"},
            {"name": "value", "type": "double"},
        ],
    }
)


@dataclass
class _FakeMsg:
    """Stand-in for `confluent_kafka.Message`."""

    _offset: int
    _key: Optional[bytes] = b"key"
    _value: Optional[bytes] = b"value"
    _error: Optional[ConfluentKafkaError] = None
    _headers: Optional[List[Tuple[str, bytes]]] = None
    _partition: int = 0
    _topic: str = TOPIC

    def error(self):
        return self._error

    def headers(self):
        return self._headers

    def key(self):
        return self._key

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def latency(self):
        return 0.5

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def timestamp(self):
        return (1, 1000 + self._offset)


def _eof_msg(offset: int) -> _FakeMsg:
    return _FakeMsg(
        offset, _error=ConfluentKafkaError(ConfluentKafkaError._PARTITION_EOF)
    )


class _FakeConsumer:
    """Stand-in for `confluent_kafka.Consumer`.

    Each `consume` call returns the next pre-loaded batch, then empty
    batches.

    """

    instances: List["_FakeConsumer"] = []
    batches: List[List[_FakeMsg]] = []

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.assigned: List[TopicPartition] = []
        self.closed = False
        self._batches = list(type(self).batches)
        type(self).instances.append(self)

    def assign(self, parts):
        self.assigned = parts

    def consume(self, num_messages, timeout):
        if self._batches:
            return self._batches.pop(0)
        return []

    def close(self):
        self.closed = True


@fixture
def fake_consumer(monkeypatch):
    _FakeConsumer.instances = []
    _FakeConsumer.batches = []
    monkeypatch.setattr(kafka_mod, "Consumer", _FakeConsumer)
    return _FakeConsumer


@dataclass
class _FakePartitionMetadata:
    id: int


@dataclass
class _FakeTopicMetadata:
    partitions: Dict[int, _FakePartitionMetadata]
    error: Optional[ConfluentKafkaError] = None


@dataclass
class _FakeClusterMetadata:
    topics: Dict[str, _FakeTopicMetadata]


class _FakeAdminClient:
    """Stand-in for `confluent_kafka.admin.AdminClient`."""

    instances: List["_FakeAdminClient"] = []
    topics: Dict[str, _FakeTopicMetadata] = {}

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.polled: List[float] = []
        self.listed: List[Tuple[str, float]] = []
        type(self).instances.append(self)

    def poll(self, timeout):
        self.polled.append(timeout)

    def list_topics(self, topic, timeout):
        self.listed.append((topic, timeout))
        known = type(self).topics
        return _FakeClusterMetadata({topic: known[topic]} if topic in known else {})


@fixture
def fake_admin(monkeypatch):
    _FakeAdminClient.instances = []
    _FakeAdminClient.topics = {}
    monkeypatch.setattr(kafka_mod, "AdminClient", _FakeAdminClient)
    return _FakeAdminClient


@dataclass
class _FakeProducer:
    """Stand-in for `confluent_kafka.Producer`."""

    config: Dict[str, Any]
    produced: List[Dict[str, Any]] = field(default_factory=list)
    flushes: int = 0

    def produce(self, **kwargs):
        self.produced.append(kwargs)

    def poll(self, timeout):
        pass

    def flush(self):
        self.flushes += 1


@fixture
def fake_producer(monkeypatch):
    producers: List[_FakeProducer] = []

    def build(config):
        producer = _FakeProducer(config)
        producers.append(producer)
        return producer

    monkeypatch.setattr(kafka_mod, "Producer", build)
    return producers


def _parts(n: int) -> Dict[int, _FakePartitionMetadata]:
    return {i: _FakePartitionMetadata(i) for i in range(n)}


def test_list_parts_one_part_per_topic_partition(fake_admin):
    fake_admin.topics = {
        "a": _FakeTopicMetadata(_parts(2)),
        "b": _FakeTopicMetadata(_parts(1)),
    }

    src = KafkaSource(["broker:9092"], ["a", "b"], add_config={"client.id": "test"})

    assert src.list_parts() == ["0-a", "1-a", "0-b"]
    (client,) = fake_admin.instances
    assert client.config == {"bootstrap.servers": "broker:9092", "client.id": "test"}
    # Auth callbacks are kicked off before listing topics.
    assert client.polled == [0]
    assert client.listed == [("a", 10.0), ("b", 10.0)]


def test_list_parts_raises_on_missing_topic(fake_admin):
    src = KafkaSource(["broker:9092"], ["missing"])

    with raises(RuntimeError, match="Kafka topic `'missing'` was not found"):
        src.list_parts()


def test_list_parts_raises_on_topic_error(fake_admin):
    err = ConfluentKafkaError(ConfluentKafkaError.UNKNOWN_TOPIC_OR_PART)
    fake_admin.topics = {"a": _FakeTopicMetadata({}, error=err)}
    src = KafkaSource(["broker:9092"], ["a"])

    with raises(RuntimeError, match="error listing partitions for Kafka topic `'a'`"):
        src.list_parts()


def test_list_parts_raises_on_no_partitions(fake_admin):
    fake_admin.topics = {"a": _FakeTopicMetadata({})}
    src = KafkaSource(["broker:9092"], ["a"])

    with raises(RuntimeError, match="no partitions listed for Kafka topic `'a'`"):
        src.list_parts()


def test_build_part_consumer_config(fake_consumer):
    src = KafkaSource(
        ["b1:9092", "b2:9092"],
        [TOPIC],
        tail=False,
        add_config={"client.id": "test", "statistics.interval.ms": "5000"},
    )

    part = src.build_part("step", f"2-{TOPIC}", None)

    (consumer,) = fake_consumer.instances
    config = dict(consumer.config)
    assert callable(config.pop("stats_cb"))
    assert config == {
        "group.id": "BYTEWAX_IGNORED",
        "enable.auto.commit": "false",
        "bootstrap.servers": "b1:9092,b2:9092",
        "enable.partition.eof": "True",
        # User config overrides the defaults.
        "statistics.interval.ms": "5000",
        "client.id": "test",
    }
    assert consumer.assigned == [TopicPartition(TOPIC, 2, OFFSET_BEGINNING)]
    assert part.snapshot() == OFFSET_BEGINNING


def test_build_part_tail_disables_partition_eof(fake_consumer):
    KafkaSource(["b:9092"], [TOPIC]).build_part("step", f"0-{TOPIC}", None)

    (consumer,) = fake_consumer.instances
    assert consumer.config["enable.partition.eof"] == "False"


def test_build_part_topic_with_dash(fake_consumer):
    KafkaSource(["b:9092"], ["my-topic-1"]).build_part("step", "3-my-topic-1", None)

    (consumer,) = fake_consumer.instances
    assert consumer.assigned == [TopicPartition("my-topic-1", 3, OFFSET_BEGINNING)]


def test_build_part_resume_state_overrides_starting_offset(fake_consumer):
    src = KafkaSource(["b:9092"], [TOPIC], starting_offset=7)

    part = src.build_part("step", f"0-{TOPIC}", 42)

    (consumer,) = fake_consumer.instances
    assert consumer.assigned == [TopicPartition(TOPIC, 0, 42)]
    assert part.snapshot() == 42


def test_build_part_starting_offset_without_resume_state(fake_consumer):
    src = KafkaSource(["b:9092"], [TOPIC], starting_offset=7)

    part = src.build_part("step", f"0-{TOPIC}", None)

    (consumer,) = fake_consumer.instances
    assert consumer.assigned == [TopicPartition(TOPIC, 0, 7)]
    assert part.snapshot() == 7


def test_next_batch_converts_messages_and_tracks_offset(fake_consumer):
    fake_consumer.batches = [
        [_FakeMsg(10, _headers=[("h", b"1")]), _FakeMsg(11, _key=None, _value=None)],
        [_FakeMsg(12)],
    ]
    part = KafkaSource(["b:9092"], [TOPIC]).build_part("step", f"0-{TOPIC}", 10)

    assert part.next_batch() == [
        KafkaSourceMessage(
            key=b"key",
            value=b"value",
            topic=TOPIC,
            headers=[("h", b"1")],
            latency=0.5,
            offset=10,
            partition=0,
            timestamp=(1, 1010),
        ),
        KafkaSourceMessage(
            key=None,
            value=None,
            topic=TOPIC,
            headers=[],
            latency=0.5,
            offset=11,
            partition=0,
            timestamp=(1, 1011),
        ),
    ]
    # Resume from the message after the last one emitted.
    assert part.snapshot() == 12

    assert [m.offset for m in part.next_batch()] == [12]
    assert part.snapshot() == 13

    # Nothing new: empty batch and the resume offset doesn't move.
    assert part.next_batch() == []
    assert part.snapshot() == 13


def test_next_batch_emits_messages_before_eof_then_stops(fake_consumer):
    fake_consumer.batches = [[_FakeMsg(0), _FakeMsg(1), _eof_msg(2), _FakeMsg(2)]]
    part = KafkaSource(["b:9092"], [TOPIC], tail=False).build_part(
        "step", f"0-{TOPIC}", None
    )

    assert [m.offset for m in part.next_batch()] == [0, 1]
    assert part.snapshot() == 2
    with raises(StopIteration):
        part.next_batch()


def test_next_batch_raises_on_error(fake_consumer):
    err = ConfluentKafkaError(ConfluentKafkaError._TRANSPORT, "broker went away")
    fake_consumer.batches = [[_FakeMsg(0), _FakeMsg(1, _error=err)]]
    part = KafkaSource(["b:9092"], [TOPIC]).build_part("step", f"0-{TOPIC}", None)

    expect = f"error consuming from Kafka topic `{TOPIC!r}`"
    with raises(RuntimeError, match=re.escape(expect)):
        part.next_batch()


def test_next_batch_emits_error_when_not_raising(fake_consumer):
    err = ConfluentKafkaError(ConfluentKafkaError._TRANSPORT, "broker went away")
    fake_consumer.batches = [[_FakeMsg(0), _FakeMsg(1, _error=err), _FakeMsg(2)]]
    part = KafkaSource(["b:9092"], [TOPIC], raise_on_errors=False).build_part(
        "step", f"0-{TOPIC}", None
    )

    first, second, third = part.next_batch()

    assert isinstance(first, KafkaSourceMessage)
    assert isinstance(second, KafkaError)
    assert second.err == err
    assert second.msg.offset == 1
    assert isinstance(third, KafkaSourceMessage)
    # Errors don't stop the offset from advancing.
    assert part.snapshot() == 3


def test_close_closes_consumer(fake_consumer):
    part = KafkaSource(["b:9092"], [TOPIC]).build_part("step", f"0-{TOPIC}", None)

    part.close()

    (consumer,) = fake_consumer.instances
    assert consumer.closed


def _lag(step_id: str, partition: int) -> Optional[float]:
    return REGISTRY.get_sample_value(
        "bytewax_kafka_consumer_lag",
        {"step_id": step_id, "topic": TOPIC, "partition": str(partition)},
    )


def _stats(partition: int, ls_offset: int) -> str:
    return json.dumps(
        {"topics": {TOPIC: {"partitions": {str(partition): {"ls_offset": ls_offset}}}}}
    )


def test_stats_callback_sets_consumer_lag(fake_consumer):
    part = KafkaSource(["b:9092"], [TOPIC]).build_part("lag_step", f"1-{TOPIC}", 40)
    stats_cb = fake_consumer.instances[0].config["stats_cb"]

    stats_cb(_stats(1, 100))

    assert _lag("lag_step", 1) == 60
    part.close()


def test_stats_callback_skips_lag_for_logical_offset(fake_consumer):
    KafkaSource(["b:9092"], [TOPIC]).build_part("no_lag_step", f"0-{TOPIC}", None)
    stats_cb = fake_consumer.instances[0].config["stats_cb"]

    # The offset is still the logical `OFFSET_BEGINNING`, so a lag
    # can't be computed.
    stats_cb(_stats(0, 100))

    assert _lag("no_lag_step", 0) is None


def test_sink_build_config(fake_producer):
    sink = KafkaSink(["b1:9092", "b2:9092"], TOPIC, add_config={"acks": "all"})

    sink.build("step", 0, 1)

    (producer,) = fake_producer
    assert producer.config == {"bootstrap.servers": "b1:9092,b2:9092", "acks": "all"}


def test_sink_write_batch_topic_selection(fake_producer):
    part = KafkaSink(["b:9092"], TOPIC).build("step", 0, 1)

    part.write_batch(
        [
            KafkaSinkMessage(b"k1", b"v1"),
            KafkaSinkMessage(
                b"k2", b"v2", topic="other", headers=[("h", b"1")], timestamp=123
            ),
        ]
    )

    (producer,) = fake_producer
    assert producer.produced == [
        {"value": b"v1", "key": b"k1", "headers": [], "topic": TOPIC, "timestamp": 0},
        {
            "value": b"v2",
            "key": b"k2",
            "headers": [("h", b"1")],
            "topic": "other",
            "timestamp": 123,
        },
    ]
    # Each batch is flushed before returning.
    assert producer.flushes == 1

    part.close()
    assert producer.flushes == 2


def test_sink_write_batch_raises_without_topic(fake_producer):
    part = KafkaSink(["b:9092"], None).build("step", 0, 1)

    with raises(RuntimeError, match="No topic to produce to"):
        part.write_batch([KafkaSinkMessage(b"k", b"v")])


def test_source_message_to_sink():
    msg = KafkaSourceMessage(
        b"k",
        b"v",
        topic=TOPIC,
        headers=[("h", b"1")],
        offset=5,
        partition=2,
        timestamp=(1, 99),
    )

    assert msg.to_sink() == KafkaSinkMessage(b"k", b"v", headers=[("h", b"1")])


def _source_msg(key: Any, value: Any, offset: int = 0) -> KafkaSourceMessage:
    return KafkaSourceMessage(
        key, value, topic=TOPIC, headers=[("h", b"1")], offset=offset, partition=1
    )


class _FailOn:
    """Deserializer that fails on one input and decodes the rest."""

    def __init__(self, bad: bytes):
        self.bad = bad

    def __call__(self, data, ctx=None):
        if data == self.bad:
            msg = f"can't decode {data!r}"
            raise ValueError(msg)
        return data.decode()


def _run_deserialize(build, inp):
    oks: List[Any] = []
    errs: List[Any] = []
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    out = build(s)
    op.output("oks", out.oks, TestingSink(oks))
    op.output("errs", out.errs, TestingSink(errs))
    run_main(flow)
    return oks, errs


def test_deserialize_key():
    good = _source_msg(b"a", b"x", offset=0)
    bad = _source_msg(b"bad", b"y", offset=1)

    oks, errs = _run_deserialize(
        lambda s: kop.deserialize_key("de", s, _FailOn(b"bad")), [good, bad]
    )

    # Everything except the key is preserved.
    assert oks == [_source_msg("a", b"x", offset=0)]
    (err,) = errs
    assert err.err.code() == ConfluentKafkaError._KEY_DESERIALIZATION
    assert "can't decode b'bad'" in err.err.str()
    assert err.msg == bad


def test_deserialize_value():
    good = _source_msg(b"a", b"x", offset=0)
    bad = _source_msg(b"b", b"bad", offset=1)

    oks, errs = _run_deserialize(
        lambda s: kop.deserialize_value("de", s, _FailOn(b"bad")), [good, bad]
    )

    assert oks == [_source_msg(b"a", "x", offset=0)]
    (err,) = errs
    assert err.err.code() == ConfluentKafkaError._VALUE_DESERIALIZATION
    assert err.msg == bad


def test_deserialize_key_and_value():
    good = _source_msg(b"a", b"x", offset=0)
    bad_key = _source_msg(b"bad", b"y", offset=1)
    bad_value = _source_msg(b"c", b"bad", offset=2)

    oks, errs = _run_deserialize(
        lambda s: kop.deserialize(
            "de",
            s,
            key_deserializer=_FailOn(b"bad"),
            val_deserializer=_FailOn(b"bad"),
        ),
        [good, bad_key, bad_value],
    )

    assert oks == [_source_msg("a", "x", offset=0)]
    # The original, undecoded message is attached to each error.
    assert [(e.err.code(), e.msg) for e in errs] == [
        (ConfluentKafkaError._KEY_DESERIALIZATION, bad_key),
        (ConfluentKafkaError._VALUE_DESERIALIZATION, bad_value),
    ]


def _run_serialize(build, inp):
    out: List[Any] = []
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", build(s), TestingSink(out))
    run_main(flow)
    return out


def test_serialize_key_value_and_both_convert_source_messages():
    ser = StringSerializer()
    inp = [
        _source_msg("k", "v"),
        KafkaSinkMessage("k2", "v2", topic="other", partition=3, timestamp=7),
    ]

    assert _run_serialize(lambda s: kop.serialize_key("ser", s, ser), inp) == [
        KafkaSinkMessage(b"k", "v", headers=[("h", b"1")]),
        KafkaSinkMessage(b"k2", "v2", topic="other", partition=3, timestamp=7),
    ]
    assert _run_serialize(lambda s: kop.serialize_value("ser", s, ser), inp) == [
        KafkaSinkMessage("k", b"v", headers=[("h", b"1")]),
        KafkaSinkMessage("k2", b"v2", topic="other", partition=3, timestamp=7),
    ]
    assert _run_serialize(
        lambda s: kop.serialize("ser", s, key_serializer=ser, val_serializer=ser), inp
    ) == [
        KafkaSinkMessage(b"k", b"v", headers=[("h", b"1")]),
        KafkaSinkMessage(b"k2", b"v2", topic="other", partition=3, timestamp=7),
    ]


def test_serialize_raises_on_serializer_error():
    def fail(obj, ctx=None):
        msg = "can't encode"
        raise ValueError(msg)

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource([_source_msg("k", "v")]))
    op.output("out", kop.serialize_value("ser", s, fail), TestingSink([]))

    with raises(BytewaxRuntimeError) as exc_info:
        run_main(flow)

    causes = []
    ex: Optional[BaseException] = exc_info.value
    while ex is not None:
        causes.append(ex)
        ex = ex.__cause__
    assert any(isinstance(c, ValueError) and "can't encode" in str(c) for c in causes)


def test_string_serde_round_trip():
    inp = [_source_msg("k1", "v1", offset=0), _source_msg("k2", "v2", offset=1)]
    serialized = _run_serialize(
        lambda s: kop.serialize(
            "ser",
            s,
            key_serializer=StringSerializer(),
            val_serializer=StringSerializer(),
        ),
        inp,
    )
    as_source = [
        _source_msg(m.key, m.value, offset=i) for i, m in enumerate(serialized)
    ]

    oks, errs = _run_deserialize(
        lambda s: kop.deserialize(
            "de",
            s,
            key_deserializer=StringDeserializer(),
            val_deserializer=StringDeserializer(),
        ),
        as_source,
    )

    assert oks == inp
    assert errs == []


def test_plain_avro_round_trip():
    readings = [{"sensor": "a", "value": 1.5}, {"sensor": "b", "value": -2.0}]
    ser = PlainAvroSerializer(AVRO_SCHEMA)
    de = PlainAvroDeserializer(AVRO_SCHEMA)

    encoded = [ser(r) for r in readings]

    assert all(isinstance(b, bytes) for b in encoded)
    assert [de(b) for b in encoded] == readings


def test_plain_avro_accepts_schema_object():
    # Building a real `Schema` needs `confluent_kafka.schema_registry`,
    # which needs the `schemaregistry` extra on confluent-kafka >= 2.7.
    schema_registry = importorskip(
        "confluent_kafka.schema_registry",
        reason="needs the confluent-kafka `schemaregistry` extra",
    )
    schema = schema_registry.Schema(AVRO_SCHEMA, "AVRO")
    reading = {"sensor": "a", "value": 3.0}

    encoded = PlainAvroSerializer(schema)(reading)

    assert PlainAvroDeserializer(schema)(encoded) == reading
    # No schema registry framing: same bytes as with a plain schema str.
    assert encoded == PlainAvroSerializer(AVRO_SCHEMA)(reading)


def test_plain_avro_deserializer_raises_on_none():
    with raises(ValueError, match="Can't deserialize None data"):
        PlainAvroDeserializer(AVRO_SCHEMA)(None)


def test_plain_avro_deserializer_accepts_str():
    schema = json.dumps({"type": "string"})

    # A `str` payload is UTF-8 encoded first; "\x06" is the Avro
    # length prefix for 3 chars.
    assert PlainAvroDeserializer(schema)("\x06abc") == "abc"


def test_plain_avro_deserialize_value_operator_errors():
    good = _source_msg(
        b"k", PlainAvroSerializer(AVRO_SCHEMA)({"sensor": "a", "value": 1.0})
    )
    bad = _source_msg(b"k", None, offset=1)

    oks, errs = _run_deserialize(
        lambda s: kop.deserialize_value("de", s, PlainAvroDeserializer(AVRO_SCHEMA)),
        [good, bad],
    )

    assert [m.value for m in oks] == [{"sensor": "a", "value": 1.0}]
    (err,) = errs
    assert err.err.code() == ConfluentKafkaError._VALUE_DESERIALIZATION
    assert err.msg == bad
