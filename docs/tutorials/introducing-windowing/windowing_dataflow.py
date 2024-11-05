"""This script demonstrates how to use windowing operators in a dataflow."""

# start-imports
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import EventClock, TumblingWindower, count_window

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
# op.inspect("parsed", parsed)
# end-parse


# start-objective-1
# Define the clock using event timestamps
def extract_timestamp(x):
    """Extract the timestamp from the data."""
    return x[1]  # Extract the timestamp from the data


clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

# Define the tumbling window of 30 minutes starting at 08:00
windower = TumblingWindower(
    length=timedelta(minutes=30),
    align_to=datetime(2024, 8, 29, 8, 0, 0, tzinfo=timezone.utc),
)

# Count the number of orders in each window
windowed = count_window(
    step_id="count_orders",
    up=parsed,
    clock=clock,
    windower=windower,
    key=lambda x: "total_orders",  # Use a constant key to aggregate all orders
)

op.inspect("windowed_count_orders", windowed.down)
# end-objective-1


# start-format-1
# Format and output the results
def format_output(item):
    """Format the output for each window."""
    key, (window_id, count) = item
    window_start = windower.align_to + timedelta(minutes=30 * window_id)
    window_end = window_start + timedelta(minutes=30)
    return f"Window {window_id}\
    ({window_start.strftime('%H:%M')}\
    - {window_end.strftime('%H:%M')}): {count} orders"


formatted = op.map("format_output_1", windowed.down, format_output)

# Inspect the output
op.inspect("formatted_objective_1", formatted)
# end-format-1
