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


class _NumberSource(StatefulSourcePartition):
    def __init__(self, i: int) -> None:
        self.i = i
        self.records = self._record_gen()

    def close(self) -> None:
        pass

    def next_awake(self) -> Optional[datetime]:
        return None

    def _record_gen(self) -> Generator[List[datetime], None, None]:
        yield [
            datetime.now(tz=timezone.utc) + timedelta(seconds=i) for i in range(self.i)
        ]

    def next_batch(self, *args, **kwargs) -> List[datetime]:
        return next(self.records)

    def snapshot(self) -> None:
        return None


class NumbersInput(FixedPartitionedSource):
    def __init__(self, i: int) -> None:
        self.i = i

    def build_part(self, *args, **kwargs) -> _NumberSource:
        return _NumberSource(self.i)

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
(
    op.input("in", flow, NumbersInput(BATCH_SIZE))
    .then(op.flat_map, "flat-map", lambda x: (x for _ in range(BATCH_COUNT)))
    .then(op.key_on, "key-on", lambda _: "x")
    .then(
        w.fold_window,
        "fold-window",
        clock_config,
        window,
        lambda: None,
        lambda s, _: s,
    )
    .then(op.filter, "filter_all", lambda _: False)
    .then(op.output, "stdout", StdOutSink())
)


def test_run_main(benchmark):
    benchmark.pedantic(run_main, args=(flow,))


def test_cluster_main(benchmark):
    benchmark.pedantic(
        cluster_main,
        args=(flow,),
        kwargs={"addresses": ["localhost:9999"], "proc_id": 0},
    )
