import json
from time import sleep

import fake_events
import pandas as pd
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic


def create_events(topic_name, servers=["localhost:9092"]):
    topic_name = topic_name
    producer = KafkaProducer(bootstrap_servers=servers)

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    for event in fake_events.generate_web_events():
        producer.send(topic_name, json.dumps(event).encode())
        # print(f"Published message to message broker. {event}")


def create_anomaly_stream(topic_name, servers=["localhost:9092"]):
    topic_name = topic_name
    producer = KafkaProducer(bootstrap_servers=servers)

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    df = pd.read_csv("examples/sample_data/ec2_metrics.csv")
    for row in df[["index", "timestamp", "value", "instance"]].to_dict("records"):
        producer.send(topic_name, json.dumps(row).encode())


if __name__ == "__main__":
    create_anomaly_stream("ec2_metrics")
