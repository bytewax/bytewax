"""Setup a dataflow for handling missing data in a stream of numbers.

This example demonstrates how to use the bytewax library to create a dataflow
that processes a stream of numbers, where every 5th number is missing (represented
by np.nan). The dataflow uses a stateful operator to impute the missing values
using a windowed mean imputation strategy.
"""

from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("timeseries")
csv_path = Path("iot_telemetry_data_1000.csv")
input_data = op.input("simulated_stream", flow, CSVSource(csv_path))
op.output("output", input_data, StdOutSink())


# from bytewax.window import EventClockConfig, TumblingWindow
# from ydata_profiling import ProfileReport
# import pandas as pd
# # parse timestamp
# def parse_time(reading_data):
#     reading_data["ts"] = datetime.fromtimestamp(float(reading_data["ts"]),
#                                                   timezone.utc)
#     return reading_data

# flow.map(parse_time)


# # remap format to tuple (device_id, reading_data)
# flow.map(lambda reading_data: (reading_data['device'], reading_data))


# # This is the accumulator function, and outputs a list of readings
# def acc_values(acc, reading):
#     acc.append(reading)
#     return acc


# # This function instructs the event clock on how to retrieve the
# # event's datetime from the input.
# def get_time(reading):
#     return reading["ts"]


# # Configure the `fold_window` operator to use the event time.
# cc = EventClockConfig(get_time, wait_for_system_duration=timedelta(seconds=30))

# # And a 5 seconds tumbling window
# align_to = datetime(2020, 1, 1, tzinfo=timezone.utc)
# wc = TumblingWindow(align_to=align_to, length=timedelta(hours=1))

# flow.fold_window("running_average", cc, wc, list, acc_values)

# flow.inspect(print)


# def profile(device_id__readings):
#     print(device_id__readings)
#     device_id, readings = device_id__readings
#     start_time = readings[0]['ts'].replace(minute=0,
#                   second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
#     df = pd.DataFrame(readings)
#     profile = ProfileReport(
#         df,
#         tsmode=True,
#         sortby="ts",
#         title=f"Sensor Readings - device: {device_id}"
#     )

#     profile.to_file(f"Ts_Profile_{device_id}-{start_time}.html")
#     return f"device {device_id} profiled at hour {start_time}"

# flow.map(profile)

# flow.output("out", StdOutput())
