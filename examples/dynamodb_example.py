import json
import logging

from datetime import datetime, timezone, timedelta

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import DynamoDBOutputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig
from bytewax.recovery import KafkaRecoveryConfig

logging.basicConfig(level=logging.DEBUG)


def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    payload = json.loads(payload_bytes)
    return str(payload["id"]), payload


# This is the accumulator function, and outputs a list of 2-tuples,
# containing the event's "value" and it's "time" (used later to print info)
def acc_values(acc, event):
    acc.append((event["value"], event["time"]))
    return acc


# This function instructs the event clock on how to retrieve the
# event's datetime from the input.
# Note that the datetime MUST be UTC. If the datetime is using a different
# representation, we would have to convert it here.
def get_event_time(event):
    return datetime.fromisoformat(event["time"])


# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))


# And a 5 seconds tumbling window, that starts at the beginning of the hour
start_at = datetime.now(timezone.utc)
start_at = start_at - timedelta(
    minutes=start_at.minute, seconds=start_at.second, microseconds=start_at.microsecond
)
wc = TumblingWindowConfig(start_at=start_at, length=timedelta(seconds=5))


# Configure the `fold_window` operator to use the event time.
cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(seconds=10))

# Calculate the average of the values for each window, and
# format the data as a map for DynamoDB
def format(event):
    key, data = event
    values = [x[0] for x in data]
    dates = [datetime.fromisoformat(x[1]) for x in data]
    return key, {
        "average": sum(values) / len(values),
        "num_events": len(values),
        "from": (min(dates) - start_at).total_seconds(),
        "to": (max(dates) - start_at).total_seconds(),
    }


flow = Dataflow()
flow.input("inp", KafkaInputConfig(brokers=["localhost:9092"], topic="input_topic"))
flow.map(deserialize)
# Here is where we use the event time processing, with
# the fold_window operator.
# The `EventClockConfig` allows us to advance our internal clock
# based on the time received in each event.
flow.fold_window("running_average", cc, wc, list, acc_values)
flow.map(format)
flow.capture(
    DynamoDBOutputConfig(
        table="example",
        primary_key="id",
    )
)

recovery_config = KafkaRecoveryConfig(
    ["127.0.0.1:9092"],
    "dynamodb",
)

if __name__ == "__main__":
    cluster_main(
        flow,
        [],
        0,
        recovery_config=recovery_config,
    )
