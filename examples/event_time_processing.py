#!/usr/bin/env python
# coding: utf-8

###################
# ---IMPORTANT--- #
###################
# To run this example you'll need a Kafka (or redpanda) cluster.
# Create a topic with using the `create_temperature_events` function
# in examples/example_utils/topics_helper.py:
#
# ```python
# from utils.topics_helper import create_temperature_events
# create_temperature_events("sensors")
# ```
#
# The events generated in the stream will be a json string with 3 keys:
# - type: a string representing the type of reading (eg: "temp")
# - value: a float representing the value of the reading
# - time: A string representing the UTC datetime the event was generated,
#         in isoformat (eg: datetime.now(timezone.utc).isoformat() )
import json
from datetime import datetime, timedelta, timezone

from bytewax.connectors.kafka import KafkaInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.window import EventClockConfig, TumblingWindow

# Define the dataflow object and kafka input.
flow = Dataflow()


flow.input("inp", KafkaInput(["localhost:9092"], ["sensors"], tail=False))


# We expect a json string that represents a reading from a sensor.
def parse_value(key__data):
    _, data = key__data
    return json.loads(data)


flow.map(parse_value)


# Divide the readings by sensor type, so that we only
# aggregate readings of the same type.
def extract_sensor_type(event):
    return event["type"], event


flow.map(extract_sensor_type)


# Here is where we use the event time processing, with
# the fold_window operator.
# The `EventClockConfig` allows us to advance our internal clock
# based on the time received in each event.


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

# And a 5 seconds tumbling window
align_to = datetime(2023, 1, 1, tzinfo=timezone.utc)
wc = TumblingWindow(align_to=align_to, length=timedelta(seconds=5))

flow.fold_window("running_average", cc, wc, list, acc_values)


# Calculate the average of the values for each window, and
# format the data to a string
def format(event):
    key, data = event
    values = [x[0] for x in data]
    dates = [datetime.fromisoformat(x[1]) for x in data]
    return (
        f"Average {key}: {sum(values) / len(values):.2f}\t"
        f"Num events: {len(values)}\t"
        f"From {min(dates).total_seconds():.2f}s\t"
        f"to {max(dates).total_seconds():.2f}s"
    )


flow.map(format)
flow.output("out", StdOutput())
