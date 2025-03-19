"""This script demonstrates how to use windowing operators in a dataflow."""

# start-imports
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.dataflow import Dataflow
from bytewax.windowing import (
    EventClock,
    SlidingWindower,
    count_window,
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
op.inspect("see_data", up)
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
op.inspect("parsed", parsed)
# end-parse


# start-objective-2
# Define the clock using event timestamps
def extract_timestamp(x):
    """Extract the timestamp from the data."""
    return x[1]  # Extract the timestamp from the data


# Define clock based on event timestamps
clock = EventClock(
    ts_getter=extract_timestamp, wait_for_system_duration=timedelta(seconds=0)
)


# Create a sliding window with an offset of 15 minutes and length of 1 hour
windower = SlidingWindower(
    length=timedelta(hours=1),
    offset=timedelta(minutes=15),
    align_to=datetime(2024, 8, 29, 8, 0, 0, tzinfo=timezone.utc),
)


# Count the number of orders in each window
windowed = count_window(
    step_id="count_orders_sliding",
    up=parsed,
    clock=clock,
    windower=windower,
    key=lambda x: "total_orders",
)


# Format and output the results
def format_output_objective_2(item):
    """Format the output for each window."""
    key, (window_id, count) = item
    window_start = windower.align_to + timedelta(minutes=15 * window_id)
    window_end = window_start + windower.length
    return f"Window {window_id}\
    ({window_start.strftime('%H:%M')} -\
    {window_end.strftime('%H:%M')}): {count} orders"


formatted_2 = op.map("format_output_sliding", windowed.down, format_output_objective_2)
op.inspect("formatted_objective_2", formatted_2)
# end-objective-2
