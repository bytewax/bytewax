import datetime as dt
import json
import pandas as pd

from feast import FeatureStore
from kafka import KafkaConsumer

from bytewax.execution import spawn_cluster
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig

# Configure the feature store for each worker to access
store = FeatureStore(repo_path=".")


def input_builder(worker_index, worker_count, resume_state):
    # Ignore state recovery
    resume_state = None

    def consume_from_kafka():
        consumer = KafkaConsumer(
            bootstrap_servers="localhost:9092", auto_offset_reset="earliest"
        )
        consumer.subscribe("drivers")
        for msg in consumer:
            event = msg.value
            yield resume_state, json.loads(event)

    return consume_from_kafka()


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

    def output_fn(panda_df):
        store.write_to_online_store("driver_daily_stats", panda_df)

        # Retrieve/print from store for demonstrative purposes
        driver_id = panda_df.iloc[0]["driver_id"]
        feature_views = store.get_online_features(
            features=[
                "driver_daily_stats:conv_rate",
                "driver_daily_stats:acc_rate",
            ],
            entity_rows=[{"driver_id": driver_id}],
        ).to_dict()
        print("New values for driver:", feature_views)

    return output_fn


def collect_events(all_events, new_events):
    all_events.extend(new_events)
    return all_events


def calculate_avg(driver_events):
    driver_id, driver_events = driver_events
    total_events = len(driver_events)
    first_event = driver_events[0]
    conv_rates = [event["conv_rate"] for event in driver_events]
    acc_rates = [event["acc_rate"] for event in driver_events]
    new_conv_avg = sum(conv_rates) / total_events
    new_acc_avg = sum(acc_rates) / total_events

    panda_df = pd.DataFrame.from_dict(
        {
            "driver_id": [driver_id],
            # Assuming 24 hour window falls on single day for timestamp
            "event_timestamp": [first_event["event_timestamp"]],
            "created": [dt.datetime.now()],
            "conv_rate": [new_conv_avg],
            "acc_rate": [new_acc_avg],
        }
    )

    return panda_df


def add_driver_id(event):
    key_bytes, payload_bytes = event
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes) if payload_bytes else None
    # Event needs to be within an array for `collect_events`
    if payload:
        return f"{payload['driver_id']}", [payload]


if __name__ == "__main__":
    # FIXME: I think we need to use the TestingClockConfig here to replicate
    #        the behavior that was in the input_builder before, but I'm not
    #        sure how to exactly replicate this with clock and window configs.
    #
    #    This is how the input was defined:
    #    return inputs.tumbling_epoch(
    #        inputs.sorted_window(
    #            consume_from_kafka(),
    #            dt.timedelta(days=1),
    #            lambda x: dt.datetime.fromisoformat(x["event_timestamp"]),
    #        ),
    #        # Make epochs daily intervals to determine daily avg
    #        dt.timedelta(days=1),
    #        # Since we're taking in all input at once,
    #        # use event timestamp for time
    #        lambda x: dt.datetime.fromisoformat(x["event_timestamp"]),
    #    )
    #
    # clock = TestingClockConfig(
    #     item_incr=dt.timedelta(days=1), start_at=dt.datetime.now()
    # )
    clock = SystemClockConfig()
    # FIXME: Just using a 0.1 seconds window here since all the data is sent
    #        in less than a second and the window doesn't close otherwise,
    #        move it to `days=1` once the original behavior is replicated
    window = TumblingWindowConfig(length=dt.timedelta(seconds=0.1))

    flow = Dataflow()
    flow.input("input", KafkaInputConfig(brokers=["localhost:9092"], topic="drivers"))
    flow.map(add_driver_id)
    flow.reduce_window("collect", clock, window, collect_events)
    flow.map(calculate_avg)
    # flow.inspect(print)
    flow.capture(ManualOutputConfig(output_builder))
    spawn_cluster(flow)
