import json
import time
from datetime import datetime

import bytewax

import pandas

import pyarrow.parquet as parquet
from bytewax import inp
from pandas import DataFrame
from pyarrow import Table

from utils import fake_events


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


def write_parquet(path__events_df):
    """Write events as partitioned Parquet in `$PWD/parquet_demo_out/`"""
    path, events_df = path__events_df
    table = Table.from_pandas(events_df)
    parquet.write_to_dataset(
        table,
        root_path="parquet_demo_out",
        partition_cols=["year", "month", "day", "page_url_path"],
    )


ec = bytewax.Executor()
# Collect 5 second tumbling windows of data and write them out as
# Parquet datasets. Arrow assigns a UUID to each worker / window's
# file so they won't clobber each other. They are further
# automatically placed in the correct directory structure based on
# date and path. `fake_events` will generate events for multiple days
# around today.
flow = ec.Dataflow(inp.tumbling_epoch(5.0, fake_events.generate_web_events()))
flow.map(json.loads)
# {"page_url_path": "/path", "event_timestamp": "2022-01-02 03:04:05", ...}
flow.map(add_date_columns)
# {"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }
flow.map(group_by_page)
# ("/path", DataFrame([{"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... })])
flow.reduce_epoch_local(append_event)
# ("/path", DataFrame([{"page_url_path": "/path", ...}, ...])
flow.map(write_parquet)
# None


if __name__ == "__main__":
    ec.build_and_run()
