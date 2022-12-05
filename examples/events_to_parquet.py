import json
from datetime import datetime, timedelta

import pandas
from pandas import DataFrame
from pyarrow import parquet, Table
from utils import fake_events

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


# `fake_events` will generate events for multiple
# days around today. Each worker will generate independent fake
# events.
def input_builder(worker_index, worker_count, resume_epoch):
    state = None

    def generator():
        for event in fake_events.generate_web_events():
            yield state, event

    return generator()


# Arrow assigns a UUID to each worker / window's file so they won't
# clobber each other. They are further automatically placed in the
# correct directory structure based on date and path.
def write_parquet(events_df):
    """Write events as partitioned Parquet in `$PWD/parquet_demo_out/`"""
    table = Table.from_pandas(events_df)
    parquet.write_to_dataset(
        table,
        root_path="parquet_demo_out",
        partition_cols=["year", "month", "day", "page_url_path"],
    )


# Each worker writes using the same code because we don't need to
# further partition because of the UUID described above.
def output_builder(worker_index, worker_count):
    return write_parquet


def add_date_columns(event):
    timestamp = datetime.fromisoformat(event["event_timestamp"])
    event["year"] = timestamp.year
    event["month"] = timestamp.month
    event["day"] = timestamp.day
    return event


def group_by_page(event):
    return event["page_url_path"], DataFrame([event])


def append_event(events_df, event_df):
    return pandas.concat([events_df, event_df])


def drop_page(page__events_df):
    page, events_df = page__events_df
    return events_df


cc = SystemClockConfig()
wc = TumblingWindowConfig(length=timedelta(seconds=5))

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(json.loads)
# {"page_url_path": "/path", "event_timestamp": "2022-01-02 03:04:05", ...}
flow.map(add_date_columns)
# {"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }
flow.map(group_by_page)
# ("/path", DataFrame([{"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }]))
flow.reduce_window("reducer", cc, wc, append_event)
# ("/path", DataFrame([{"page_url_path": "/path", ...}, ...]))
flow.map(drop_page)
# DataFrame([{"page_url_path": "/path", ...}, ...])
flow.capture(ManualOutputConfig(output_builder))


if __name__ == "__main__":
    # run_main(flow)
    spawn_cluster(flow)
