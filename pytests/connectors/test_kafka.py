import re
import uuid
from concurrent.futures import wait
from typing import Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
)
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.testing import TestingSink, TestingSource, poll_next_batch, run_main
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, raises


@fixture
def kafka_config(kafka_server):
    return {"bootstrap.servers": kafka_server}


@fixture
def tmp_topic(kafka_config, request):
    client = AdminClient(kafka_config)
    topic_name = f"pytest_{request.node.name}_{uuid.uuid4()}"
    wait(
        # 3 partitions.
        client.create_topics([NewTopic(topic_name, 3)], operation_timeout=5.0).values()
    )
    yield topic_name
    wait(client.delete_topics([topic_name], operation_timeout=5.0).values())


tmp_topic1 = tmp_topic
tmp_topic2 = tmp_topic


def as_k_v(m: KafkaSourceMessage) -> Tuple[bytes, bytes]:
    return m.key, m.value


def test_input(kafka_server, kafka_config, tmp_topic1, tmp_topic2):
    topics = [tmp_topic1, tmp_topic2]
    producer = Producer(kafka_config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key)
            inp.append((key, value))
    producer.flush()
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource([kafka_server], topics, tail=False, add_config=kafka_config),
    )
    vals = op.map("vals", s, as_k_v)
    op.output("out", vals, TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_input_resume_state(kafka_server, kafka_config, tmp_topic):
    topics = [tmp_topic]
    partition = 0
    producer = Producer(kafka_config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key, partition=partition)
            inp.append((key, value))
    producer.flush()

    inp = KafkaSource(
        [kafka_server], topics, batch_size=1, tail=False, add_config=kafka_config
    )
    part = inp.build_part("test", f"{partition}-{tmp_topic}", None)
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-0", b"value-0-0")]
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-1", b"value-0-1")]
    resume_state = part.snapshot()
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    part.close()

    inp = KafkaSource([kafka_server], topics, tail=False, add_config=kafka_config)
    part = inp.build_part("test", f"{partition}-{tmp_topic}", resume_state)
    assert part.snapshot() == resume_state
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    with raises(StopIteration):
        poll_next_batch(part)
    part.close()


def test_input_raises_on_topic_not_exist(kafka_server, kafka_config):
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource(
            [kafka_server], ["missing-topic"], tail=False, add_config=kafka_config
        ),
    )
    op.output("out", s, TestingSink(out))

    with raises(BytewaxRuntimeError):
        run_main(flow)


def test_input_raises_on_str_brokers(kafka_server, tmp_topic):
    expect = "brokers must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource(kafka_server, [tmp_topic], tail=False)


def test_input_raises_on_str_topics(kafka_server, tmp_topic):
    expect = "topics must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource([kafka_server], tmp_topic, tail=False)


def test_output(kafka_server, kafka_config, tmp_topic):
    flow = Dataflow("test_df")

    inp = [
        KafkaSinkMessage(b"key-0-0", b"value-0-0", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-1", b"value-0-2", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-2", b"value-0-2", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([kafka_server], tmp_topic, add_config=kafka_config))

    run_main(flow)

    group_config = kafka_config.copy()
    group_config["group.id"] = "BYTEWAX_UNIT_TEST"
    # Don't leave around a consumer group for this.
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(tmp_topic)
    topic_metadata = cluster_metadata.topics[tmp_topic]
    # Assign does not activate consumer grouping.
    consumer.assign(
        [
            TopicPartition(tmp_topic, i, OFFSET_BEGINNING)
            for i in topic_metadata.partitions.keys()
        ]
    )
    out = []
    for msg in consumer.consume(num_messages=100, timeout=5.0):
        if msg.error() is not None and msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        assert msg.error() is None
        out.append((msg.key(), msg.value()))
    consumer.close()

    assert sorted(out) == sorted(list(map(as_k_v, inp)))
