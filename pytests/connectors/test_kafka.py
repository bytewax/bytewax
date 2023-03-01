import uuid
from concurrent.futures import wait

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark

from bytewax.connectors.kafka import KafkaInput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import TestingOutputConfig


@fixture
def tmp_topic(request):
    config = {
        # For some reason Redpanda does not bind to IPv6 and my
        # definition of `localhost` returns an IPv6 address first, so
        # force usage of IPv4.
        "bootstrap.servers": "127.0.0.1",
    }
    client = AdminClient(config)
    topic_name = f"pytest_{request.node.name}_{uuid.uuid4()}"
    wait(
        client.create_topics([NewTopic(topic_name, -1)], operation_timeout=5.0).values()
    )
    yield topic_name
    wait(client.delete_topics([topic_name], operation_timeout=5.0).values())


tmp_topic1 = tmp_topic
tmp_topic2 = tmp_topic


@mark.skip("Don't run unless you have a Kafka cluster setup on 127.0.0.1")
def test_kafka_input(tmp_topic1, tmp_topic2):
    config = {
        "bootstrap.servers": "127.0.0.1",
    }
    topics = [tmp_topic1, tmp_topic2]
    prod = Producer(config)
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            prod.produce(topic, value, key)
    prod.flush()

    flow = Dataflow()

    flow.input("inp", KafkaInput(["localhost"], topics, tail=False))

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == [
        (b"key-0-0", b"value-0-0"),
        (b"key-0-1", b"value-0-1"),
        (b"key-0-2", b"value-0-2"),
        (b"key-1-0", b"value-1-0"),
        (b"key-1-1", b"value-1-1"),
        (b"key-1-2", b"value-1-2"),
    ]
