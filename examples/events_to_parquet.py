import json
from datetime import datetime, timedelta
from typing import Any, List, Optional

# pip install pandas pyarrow fake-web-events
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
from fake_web_events import Simulation
from pyarrow import Table, parquet


class SimulatedPartition(StatefulSourcePartition):
    def __init__(self):
        self.events = Simulation(user_pool_size=5, sessions_per_day=100).run(
            duration_seconds=10
        )

    def next_batch(self) -> List[Any]:
        return [json.dumps(next(self.events))]

    def snapshot(self) -> Any:
        return None


class FakeWebEventsSource(FixedPartitionedSource):
    def list_parts(self) -> List[str]:
        return ["singleton"]

    def build_part(
        self, _step_id: str, for_part: str, resume_state: Optional[int]
    ) -> SimulatedPartition:
        assert for_part == "singleton"
        assert resume_state is None
        return SimulatedPartition()


class ParquetPartition(StatefulSinkPartition):
    def write_batch(self, batch: List[Table]) -> None:
        for table in batch:
            parquet.write_to_dataset(
                table,
                root_path="parquet_demo_out",
                partition_cols=["year", "month", "day", "page_url_path"],
            )

    def snapshot(self) -> Any:
        return None


class ParquetSink(FixedPartitionedSink):
    def list_parts(self) -> List[str]:
        return ["singleton"]

    def assign_part(self, item_key: str) -> str:
        return "singleton"

    def build_part(
        self, _step_id: str, for_part: str, resume_state: Any
    ) -> ParquetPartition:
        return ParquetPartition()


def add_date_columns(event: dict) -> dict:
    timestamp = datetime.fromisoformat(event["event_timestamp"])
    event["year"] = timestamp.year
    event["month"] = timestamp.month
    event["day"] = timestamp.day
    return event


flow = Dataflow("events_to_parquet")
stream = op.input("input", flow, FakeWebEventsSource())
stream = op.map("load_json", stream, json.loads)
# {"page_url_path": "/path", "event_timestamp": "2022-01-02 03:04:05", ...}
stream = op.map("add_date_columns", stream, add_date_columns)
# {"page_url_path": "/path", "year": 2022, "month": 1, "day": 5, ... }
keyed_stream = op.key_on(
    "group_by_page", stream, lambda record: record["page_url_path"]
)
# ("/path", {"page_url_path": "/path", "year": 2022, "month": 1, ...})
batched_stream = op.collect(
    "batch_records", keyed_stream, max_size=50, timeout=timedelta(seconds=2)
)
# ("/path", [{"page_url_path": "/path",...}, ...])
arrow_stream = op.map(
    "arrow_table",
    batched_stream,
    lambda keyed_batch: (keyed_batch[0], Table.from_pylist(keyed_batch[1])),
)
# ("/path", pyarrow.Table(event_id: [["/path", ...,...]], ...)
op.output("out", arrow_stream, ParquetSink())
