import json
from datetime import datetime, timedelta

# pip install pandas pyarrow fake-web-events
import pandas
from fake_web_events import Simulation
from pandas import DataFrame
from pyarrow import parquet, Table

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import CustomPartInput
from bytewax.outputs import ManualOutputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig


class FakeWebEventsInput(CustomPartInput):
    def list_keys(self):
        return ["singleton"]

    def build_part(self, for_key, resume_state):
        assert for_key == "singleton"
        assert resume_state is None

        simulation = Simulation(user_pool_size=5, sessions_per_day=100)

        for event in simulation.run(duration_seconds=10):
            yield None, json.dumps(event)


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
flow.input("input", FakeWebEventsInput())
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
