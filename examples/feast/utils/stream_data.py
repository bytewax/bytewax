import json

import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic


def create_stream(topic_name, servers=["localhost:9092"]):
    topic_name = topic_name
    producer = KafkaProducer(bootstrap_servers=servers)

    try:
        # Create Kafka topic
        topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
        admin = KafkaAdminClient(bootstrap_servers=servers)
        admin.create_topics([topic])
    except Exception:
        print(f"Topic {topic_name} is already created")

    # Sort values to support tumbling_epoch for input building
    df = pd.read_parquet("data/driver_stats.parquet").sort_values(by="event_timestamp")
    for row in df[["driver_id", "event_timestamp", "conv_rate", "acc_rate", "created"]].to_dict('records'):
        row["event_timestamp"] = row["event_timestamp"].strftime('%Y-%m-%d %H:%M:%S')
        row["created"] = row["created"].strftime('%Y-%m-%d %H:%M:%S')
        producer.send(topic_name, json.dumps(row).encode())

if __name__ == "__main__":
    create_stream('drivers')