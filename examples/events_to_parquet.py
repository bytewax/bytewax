import json
from datetime import datetime, timedelta, timezone

# pip install pandas pyarrow fake-web-events
import pandas
from fake_web_events import Simulation
from pandas import DataFrame
from pyarrow import parquet, Table

from bytewax.dataflow import Dataflow
from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.outputs import PartitionedOutput, StatefulSink
from bytewax.window import SystemClockConfig, TumblingWindow


class SimulatedSource(StatefulSource):
    def __init__(self):
        self.events = Simulation(user_pool_size=5, sessions_per_day=100).run(
            duration_seconds=10
        )

    def next(self):
        return json.dumps(next(self.events))

    def snapshot(self):
        pass

    def close(self):
        pass


class FakeWebEventsInput(PartitionedInput):
    def list_parts(self):
        return {"singleton"}

    def build_part(self, for_key, resume_state):
        assert for_key == "singleton"
        assert resume_state is None
        return SimulatedSource()


class ParquetSink(StatefulSink):
    def write(self, value):
        table = Table.from_pandas(value)
        parquet.write_to_dataset(
            table,
            root_path="parquet_demo_out",
            partition_cols=["year", "month", "day", "page_url_path"],
        )

    def snapshot(self):
        pass

    def close(self):
        pass


class ParquetOutput(PartitionedOutput):
    def list_parts(self):
        return {"singleton"}

    def assign_part(self, item_key):
        return "singleton"

    def build_part(self, for_part, resume_state):
        return ParquetSink()


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
flow.input("input", FakeWebEventsInput())
flow.map(json.loads)
# {"page_url_path": "/path", "event_timestamp": "2022-01-02 03:04:05", ...}
flow.map(add_date_columns)
# {"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }
flow.map(group_by_page)
# ("/path", DataFrame([{
#     "page_url_path": "/path",
#     "year": 2022,
#     "month": 1,
#     "day": 5,
#     ...
# }]))
flow.reduce_window("reducer", cc, wc, append_event)
# ("/path", DataFrame([{"page_url_path": "/path", ...}, ...]))
flow.output("out", ParquetOutput())
