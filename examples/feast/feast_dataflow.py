import json
import datetime as dt
import pandas as pd

from kafka import KafkaConsumer

from bytewax import Dataflow, cluster_main, inputs

from feast import FeatureStore

# Configure the feature store for each worker to access
store = FeatureStore(repo_path=".")


def input_builder(worker_index, worker_count):
    def consume_from_kafka():
        consumer = KafkaConsumer(bootstrap_servers="localhost:9092", auto_offset_reset="earliest")
        consumer.subscribe("drivers")
        for msg in consumer:
            yield json.loads(msg.value)

    return inputs.tumbling_epoch(
        inputs.sorted_window(
            consume_from_kafka(),
            dt.timedelta(days=1),
            lambda x: dt.datetime.fromisoformat(x["event_timestamp"]),
        ),
        # Make epochs daily intervals to determine daily avg
        dt.timedelta(days=1),
        # Since we're taking in all input at once, use event timestamp for time
        lambda x: dt.datetime.fromisoformat(x["event_timestamp"]),
    )


def output_builder(worker_index, worker_count):
    """Write the Driver update to the Feast online store
    These driver updates are an aggregate window of data calculated
    by Bytewax
    Example hourly event:
        {
            "driver_id": [1001],
            "event_timestamp": [datetime.now()],
            "created": [datetime.now()],
            "conv_rate": [1.0],
            "acc_rate": [1.0],
        }
    """

    def output_fn(epoch_dataframe):
        panda_df = epoch_dataframe[1]
        store.write_to_online_store("driver_daily_stats", panda_df)
        driver_id = panda_df.iloc[0]["driver_id"]

        # Retrieve/print from store for demonstrative purposes
        feature_views = store.get_online_features(
            features=[
                "driver_daily_stats:conv_rate",
                "driver_daily_stats:acc_rate",
            ],    
            entity_rows=[{"driver_id": driver_id}]
        ).to_dict()
        print("New values for driver:", feature_views)

    return output_fn

def collect_events(all_events, event):
    all_events.extend(event)
    return all_events

def calculate_avg(driver_events):
    driver_events = driver_events[1]
    total_events = len(driver_events)
    e = driver_events[0]
    conv_rates = [event["conv_rate"] for event in driver_events]
    acc_rates = [event["acc_rate"] for event in driver_events]
    new_conv_avg = sum(conv_rates)/total_events
    new_acc_avg = sum(acc_rates)/total_events

    event_df = pd.DataFrame.from_dict(
        {
            "driver_id": [e["driver_id"]],
            # Assuming 24 hour window falls on single day for timestamp
            "event_timestamp": [e["event_timestamp"]],
            "created": [dt.datetime.now()],
            "conv_rate": [new_conv_avg],
            "acc_rate": [new_acc_avg],
        }
    )

    return event_df

def add_driver_id(record):
    # Record needs to be an array for `collect_events`
    return record["driver_id"], [record]


if __name__ == "__main__":
    flow = Dataflow()
    flow.map(add_driver_id)
    flow.reduce_epoch(collect_events)
    flow.map(calculate_avg)
    flow.capture()
    cluster_main(
        flow,
        input_builder,
        output_builder,
        [],  # addresses
        0,  # process id
        1,  # number of workers
    )
