import concurrent.futures
import json
import math
from datetime import datetime, timedelta, timezone
from random import random
from time import sleep

# pip install pandas confluent-kafka fake-web-events
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fake_web_events import Simulation


def create_local_kafka_producer(topic_name, servers=["localhost:9092"]):
    config = {
        "bootstrap.servers": ",".join(servers),
    }

    # Create Kafka topic
    admin = AdminClient(config)
    topic = NewTopic(topic_name, num_partitions=3)
    for future in admin.create_topics([topic]).values():
        concurrent.futures.wait([future])

    return Producer(config)


def create_temperature_events(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)
    for i in range(10):
        dt = datetime.now(timezone.utc)
        if random() > 0.8:
            dt = dt - timedelta(minutes=random())
        event = {"type": "temp", "value": i, "time": dt.isoformat()}
        producer.produce(topic_name, json.dumps(event).encode("utf-8"))
        sleep(random())

    producer.flush()


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
        producer.produce(topic_name, json.dumps(event).encode("utf-8"))
        sleep(random())

    producer.flush()


def create_fake_events(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)

    simulation = Simulation(user_pool_size=5, sessions_per_day=100)
    for event in simulation.run(duration_seconds=10):
        producer.produce(topic_name, json.dumps(event).encode())

    producer.flush()


def create_anomaly_stream(topic_name, servers=["localhost:9092"]):
    producer = create_local_kafka_producer(topic_name)

    df = pd.read_csv("examples/sample_data/ec2_metrics.csv")
    for row in df[["index", "timestamp", "value", "instance"]].to_dict("records"):
        producer.produce(topic_name, json.dumps(row).encode())

    producer.flush()


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
        producer.produce(topic_name, json.dumps(row).encode())

    producer.flush()
