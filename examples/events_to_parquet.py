import json
from datetime import datetime, timedelta, timezone

# pip install pandas pyarrow fake-web-events
import pandas
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from bytewax.window import SystemClockConfig, TumblingWindow
from fake_web_events import Simulation
from pandas import DataFrame
from pyarrow import Table, parquet


class SimulatedPartition(StatefulSourcePartition):
    def __init__(self):
        self.events = Simulation(user_pool_size=5, sessions_per_day=100).run(
            duration_seconds=10
        )

    def next_batch(self):
        return [json.dumps(next(self.events))]

    def snapshot(self):
        return None


class FakeWebEventsSource(FixedPartitionedSource):
    def list_parts(self):
        return ["singleton"]

    def build_part(self, for_key, resume_state):
        assert for_key == "singleton"
        assert resume_state is None
        return SimulatedPartition()


class ParquetPartition(StatefulSinkPartition):
    def write_batch(self, batch):
        for (_metadata, value) in batch:
            table = Table.from_pandas(value)
            parquet.write_to_dataset(
                table,
                root_path="parquet_demo_out",
                partition_cols=["year", "month", "day", "page_url_path"],
            )

    def snapshot(self):
        return None


class ParquetSink(FixedPartitionedSink):
    def list_parts(self):
        return ["singleton"]

    def assign_part(self, item_key):
        return "singleton"

    def build_part(self, for_part, resume_state):
        return ParquetPartition()


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
wc = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

flow = Dataflow()
flow.input("input", FakeWebEventsSource())
flow.map("load_json", json.loads)
# {"page_url_path": "/path", "event_timestamp": "2022-01-02 03:04:05", ...}
flow.map("add_date_columns", add_date_columns)
# {"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }
flow.map("group_by_page", group_by_page)
# ("/path", DataFrame([{
#     "page_url_path": "/path",
#     "year": 2022,
#     "month": 1,
#     "day": 5,
#     ...
# }]))
flow.reduce_window("reducer", cc, wc, append_event)
# ("/path", DataFrame([{"page_url_path": "/path", ...}, ...]))
flow.output("out", ParquetSink())
