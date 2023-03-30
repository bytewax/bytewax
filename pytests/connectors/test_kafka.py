import os
import uuid
from concurrent.futures import wait

from confluent_kafka import (
    Consumer,
    KafkaError,
    OFFSET_BEGINNING,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark, raises

from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
from bytewax.testing import poll_next, TestingInput, TestingOutput

pytestmark = mark.skipif(
    "TEST_KAFKA_BROKER" not in os.environ,
    reason="Set `TEST_KAFKA_BROKER` env var to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER")


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

    flow = Dataflow()

    flow.input("inp", KafkaInput([KAFKA_BROKER], topics, tail=False))

    out = []
    flow.output("out", TestingOutput(out))

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

    inp = KafkaInput([KAFKA_BROKER], topics, tail=False)
    part = inp.build_part(f"{partition}-{tmp_topic}", None)
    assert poll_next(part) == (b"key-0-0", b"value-0-0")
    assert poll_next(part) == (b"key-0-1", b"value-0-1")
    resume_state = part.snapshot()
    assert poll_next(part) == (b"key-0-2", b"value-0-2")
    part.close()

    inp = KafkaInput([KAFKA_BROKER], topics, tail=False)
    part = inp.build_part(f"{partition}-{tmp_topic}", resume_state)
    assert part.snapshot() == resume_state
    assert poll_next(part) == (b"key-0-2", b"value-0-2")
    with raises(StopIteration):
        poll_next(part)
    part.close()


def test_input_raises_on_topic_not_exist():
    flow = Dataflow()

    flow.input("inp", KafkaInput([KAFKA_BROKER], ["missing-topic"], tail=False))

    out = []
    flow.output("out", TestingOutput(out))

    with raises(Exception) as exinfo:
        run_main(flow)

    assert str(exinfo.value) == (
        "error listing partitions for Kafka topic `'missing-topic'`: "
        "Broker: Unknown topic or partition"
    )


def test_input_raises_on_str_brokers(tmp_topic):
    with raises(TypeError) as exinfo:
        KafkaInput(KAFKA_BROKER, [tmp_topic], tail=False)

    assert str(exinfo.value) == "brokers must be an iterable and not a string"


def test_input_raises_on_str_topics(tmp_topic):
    with raises(TypeError) as exinfo:
        KafkaInput([KAFKA_BROKER], tmp_topic, tail=False)

    assert str(exinfo.value) == "topics must be an iterable and not a string"


def test_output(tmp_topic):
    flow = Dataflow()

    inp = [
        (b"key-0-0", b"value-0-0"),
        (b"key-0-1", b"value-0-1"),
        (b"key-0-2", b"value-0-2"),
    ]
    flow.input("inp", TestingInput(inp))

    flow.output("out", KafkaOutput([KAFKA_BROKER], tmp_topic))

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
