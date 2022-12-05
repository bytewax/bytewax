import json
import math
from datetime import datetime, timedelta, timezone
from random import random
from time import sleep

import fake_events
import pandas as pd
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic


def create_local_kafka_producer(topic_name, servers=["localhost:9092"]):
    topic_name = topic_name
    producer = KafkaProducer(bootstrap_servers=servers)

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    return producer


def create_temperature_events(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)
    for i in range(10):
        dt = datetime.now(timezone.utc)
        if random() > 0.8:
            dt = dt - timedelta(minutes=random())
        event = {"type": "temp", "value": i, "time": dt.isoformat()}
        producer.send(topic_name, json.dumps(event).encode("utf-8"))
        sleep(random())


def create_sensor_events(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name, servers)
    for i in range(10):
        dt = datetime.now(timezone.utc)
        if random() > 0.8:
            dt = dt - timedelta(minutes=random())
        event = {
            "id": str(math.floor(random() * 10)),
            "value": i,
            "time": dt.isoformat(),
        }
        producer.send(topic_name, json.dumps(event).encode("utf-8"))
        sleep(random())


def create_fake_events(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)

    for event in fake_events.generate_web_events():
        producer.send(topic_name, json.dumps(event).encode())
        # print(f"Published message to message broker. {event}")


def create_anomaly_stream(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)

    df = pd.read_csv("examples/sample_data/ec2_metrics.csv")
    for row in df[["index", "timestamp", "value", "instance"]].to_dict("records"):
        producer.send(topic_name, json.dumps(row).encode())


def create_driver_stream(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)

    # Sort values to support tumbling_epoch for input building
    df = pd.read_parquet("examples/sample_data/driver_stats.parquet").sort_values(
        by="event_timestamp"
    )
    for row in df[
        ["driver_id", "event_timestamp", "conv_rate", "acc_rate", "created"]
    ].to_dict("records"):
        row["event_timestamp"] = row["event_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        row["created"] = row["created"].strftime("%Y-%m-%d %H:%M:%S")
        producer.send(topic_name, json.dumps(row).encode())


if __name__ == "__main__":
    create_anomaly_stream("ec2_metrics")
