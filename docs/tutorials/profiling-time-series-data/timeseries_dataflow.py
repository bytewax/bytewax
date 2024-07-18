"""Setup a dataflow for profiling time series data.

This script sets up a dataflow that reads in a CSV file containing time series data
and profiles the data in hourly windows. The data is read in from a CSV file and
parsed to extract the timestamp. The data is then windowed into hourly windows and
accumulated. The accumulated data is then profiled using the ydata_profiling library
and the profile report is output to a file.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
import pandas as pd
from bytewax.connectors.files import CSVSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import windowing as wop
from bytewax.operators.windowing import EventClock, TumblingWindower
from ydata_profiling import ProfileReport  # type: ignore

# Initialize dataflow
flow = Dataflow("timeseries")
csv_path = Path("iot_telemetry_data_1000.csv")
input_data = op.input("simulated_stream", flow, CSVSource(csv_path))


# Parse timestamps in data
def parse_time(reading_data):
    """Parse the timestamp in the reading data."""
    reading_data["ts"] = datetime.fromtimestamp(float(reading_data["ts"]), timezone.utc)
    return reading_data


parse_time_step = op.map("parse_time", input_data, parse_time)
map_tuple = op.map(
    "tuple_map",
    parse_time_step,
    lambda reading_data: (reading_data["device"], reading_data),
)


# Accumulator function
def acc_values():
    """Initialize the accumulator for the windowed data."""
    return []


def accumulate(acc, reading):
    """Accumulate the readings in the window."""
    acc.append(reading)
    return acc


def merge_acc(acc1, acc2):
    """Merge two accumulators together."""
    return acc1 + acc2


def output_profile(acc):
    """Output a profile report for the accumulated data."""
    df = pd.DataFrame(acc)
    profile = ProfileReport(df, title="Profiling Report")
    profile.to_file(f"profile_{datetime.now(tz=timezone.utc).isoformat()}.html")
    return acc


# Get timestamp from reading
def get_time(reading):
    """Get the timestamp from the reading."""
    return reading["ts"]


# Configure windowing
event_time_config = EventClock(get_time, wait_for_system_duration=timedelta(seconds=30))
align_to = datetime(2020, 1, 1, tzinfo=timezone.utc)
clock_config = TumblingWindower(align_to=align_to, length=timedelta(hours=1))

# Collect windowed data
windowed_data = wop.fold_window(
    "windowed_data",
    map_tuple,
    clock=event_time_config,
    windower=clock_config,
    builder=acc_values,
    folder=accumulate,
    merger=merge_acc,
)


# Process windowed data and generate profile report
def fold_and_profile(acc, reading):
    """Fold the windowed data and output a profile report."""
    acc = accumulate(acc, reading)
    if len(acc) > 0:  # Perform profiling if there are accumulated readings
        output_profile(acc)
    return acc


folded = op.fold_final("acc_values", windowed_data.down, acc_values, fold_and_profile)

# Output results
op.output("output", folded, StdOutSink())
