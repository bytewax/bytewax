import os
import re
import uuid
from concurrent.futures import wait

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSink, KafkaSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, poll_next_batch, run_main
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark, raises

pytestmark = mark.skipif(
    "TEST_KAFKA_BROKER" not in os.environ,
    reason="Set `TEST_KAFKA_BROKER` env var to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")


@fixture
def tmp_topic(request):
    config = {
        "bootstrap.servers": KAFKA_BROKER,
    }
    client = AdminClient(config)
    topic_name = f"pytest_{request.node.name}_{uuid.uuid4()}"
    wait(
        # 3 partitions.
        client.create_topics([NewTopic(topic_name, 3)], operation_timeout=5.0).values()
    )
    yield topic_name
    wait(client.delete_topics([topic_name], operation_timeout=5.0).values())


tmp_topic1 = tmp_topic
tmp_topic2 = tmp_topic


def test_input(tmp_topic1, tmp_topic2):
    config = {
        "bootstrap.servers": KAFKA_BROKER,
    }
    topics = [tmp_topic1, tmp_topic2]
    producer = Producer(config)
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
    s = op.input("inp", flow, KafkaSource([KAFKA_BROKER], topics, tail=False))
    op.output("out", s, TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_input_resume_state(tmp_topic):
    config = {
        "bootstrap.servers": KAFKA_BROKER,
    }
    topics = [tmp_topic]
    partition = 0
    producer = Producer(config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key, partition=partition)
            inp.append((key, value))
    producer.flush()

    inp = KafkaSource([KAFKA_BROKER], topics, tail=False)
    part = inp.build_part("test", f"{partition}-{tmp_topic}", None)
    assert poll_next_batch(part) == [(b"key-0-0", b"value-0-0")]
    assert poll_next_batch(part) == [(b"key-0-1", b"value-0-1")]
    resume_state = part.snapshot()
    assert poll_next_batch(part) == [(b"key-0-2", b"value-0-2")]
    part.close()

    inp = KafkaSource([KAFKA_BROKER], topics, tail=False)
    part = inp.build_part("test", f"{partition}-{tmp_topic}", resume_state)
    assert part.snapshot() == resume_state
    assert poll_next_batch(part) == [(b"key-0-2", b"value-0-2")]
    with raises(StopIteration):
        poll_next_batch(part)
    part.close()


def test_input_raises_on_topic_not_exist():
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp", flow, KafkaSource([KAFKA_BROKER], ["missing-topic"], tail=False)
    )
    op.output("out", s, TestingSink(out))

    expect = "Broker: Unknown topic or partition"
    with raises(Exception, match=re.escape(expect)):
        run_main(flow)


def test_input_raises_on_str_brokers(tmp_topic):
    expect = "brokers must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource(KAFKA_BROKER, [tmp_topic], tail=False)


def test_input_raises_on_str_topics(tmp_topic):
    expect = "topics must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource([KAFKA_BROKER], tmp_topic, tail=False)


def test_output(tmp_topic):
    flow = Dataflow("test_df")

    inp = [
        (b"key-0-0", b"value-0-0"),
        (b"key-0-1", b"value-0-1"),
        (b"key-0-2", b"value-0-2"),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([KAFKA_BROKER], tmp_topic))

    run_main(flow)

    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "BYTEWAX_UNIT_TEST",
        # Don't leave around a consumer group for this.
        "enable.auto.commit": "false",
        "enable.partition.eof": "true",
    }
    consumer = Consumer(config)
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

    assert sorted(out) == sorted(inp)
