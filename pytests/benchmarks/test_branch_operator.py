from datetime import datetime, timedelta, timezone
from typing import Generator, List, Optional

import bytewax.operators as op
import bytewax.operators.window as w
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.testing import cluster_main, run_main

BATCH_SIZE = 100_000
BATCH_COUNT = 10


class _BatchNumberSource(StatefulSourcePartition):
    def __init__(self, i: int) -> None:
        self.i = i
        self.records = self._record_gen()

    def close(self) -> None:
        pass

    def next_awake(self) -> Optional[datetime]:
        return None

    def _record_gen(self) -> Generator[List[int], None, None]:
        yield list(range(0, self.i))

    def next_batch(self, *args, **kwargs) -> List[int]:
        return next(self.records)

    def snapshot(self) -> None:
        return None


class BatchNumbersInput(FixedPartitionedSource):
    def __init__(self, i: int) -> None:
        self.i = i

    def build_part(self, *args, **kwargs) -> _BatchNumberSource:
        return _BatchNumberSource(self.i)

    def list_parts(self) -> List[str]:
        return ["single"]


clock_config = w.EventClockConfig(
    dt_getter=lambda x: x,
    wait_for_system_duration=timedelta(seconds=0),
)
window = w.TumblingWindow(
    align_to=datetime(2022, 1, 1, tzinfo=timezone.utc), length=timedelta(minutes=1)
)

flow = Dataflow("bench")
inp = op.input("in", flow, BatchNumbersInput(BATCH_SIZE))
batches = op.flat_map("flat-map", inp, lambda x: (x for _ in range(BATCH_COUNT)))
branch_out = op.branch("evens_and_odds", batches, lambda x: x / 2 == 0)
merged = op.merge("merge_streams", branch_out.trues, branch_out.falses)
op.output("stdout", merged, StdOutSink())


def test_branch_run_main(benchmark):
    benchmark.pedantic(run_main, args=(flow,))


def test_branch_cluster_main(benchmark):
    benchmark.pedantic(
        cluster_main,
        args=(flow,),
        kwargs={"addresses": ["localhost:9999"], "proc_id": 0},
    )
