"""This script demonstrates how to use windowing operators in a dataflow."""

# start-imports
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.dataflow import Dataflow
from bytewax.windowing import (
    EventClock,
    SessionWindower,
    collect_window,
)

# end-imports

# start-dataflow
# Create a new dataflow
flow = Dataflow("windowing_operators_examples")
# end-dataflow

# start-input
# Input stream
csv_file_path = Path("smoothie_order_l.csv")
up = op.input("orders", flow, CSVSource(csv_file_path))
# op.inspect("see_data", up)
# end-input


# start-parse
# Parse the CSV data
def parse_order(row):
    """Parse the order data from the CSV."""
    order_id = int(row["order_id"])
    time = datetime.strptime(row["time"], "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    order_requested = row["order_requested"]
    ingredients = row["ingredients"]
    return (order_id, time, order_requested, ingredients)


parsed = op.map("parse_order", up, parse_order)
# op.inspect("parsed", parsed)
keyed = op.key_on("key_on", parsed, lambda x: "all_orders")
# op.inspect("keyed", keyed)
# end-parse


# start-objective-3
# Define the clock using event timestamps
def extract_timestamp(x):
    """Extract the timestamp from the data."""
    return x[1]  # Extract the timestamp from the data


clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = SessionWindower(gap=timedelta(minutes=5))

# Count the number of orders in each window
windowed = collect_window(
    step_id="collect_sessions",
    up=keyed,
    clock=clock,
    windower=windower,
)

# op.inspect("windowed_count_orders", windowed.down)


def format_output(item):
    """Format the output."""
    key, (window_id, orders) = item
    order_ids = [order[0] for order in orders]
    timestamps = [order[1] for order in orders]
    session_start = min(timestamps).strftime("%H:%M")
    session_end = max(timestamps).strftime("%H:%M")
    return f"Session {window_id}\
    ({session_start} - {session_end}): Order ID {order_ids}"


formatted = op.map("format_output", windowed.down, format_output)

# Inspect the output
op.inspect("output", formatted)
# end-objective-3
