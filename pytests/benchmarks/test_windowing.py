from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.window as w
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import BatchInput, cluster_main, run_main

BATCH_SIZE = 100_000
BATCH_COUNT = 10

clock_config = w.EventClockConfig(
    dt_getter=lambda x: x,
    wait_for_system_duration=timedelta(seconds=0),
)
window = w.TumblingWindow(
    align_to=datetime(2022, 1, 1, tzinfo=timezone.utc), length=timedelta(minutes=1)
)

records = [
    datetime.now(tz=timezone.utc) + timedelta(seconds=i) for i in range(BATCH_SIZE)
]
flow = Dataflow("bench")
(
    op.input("in", flow, BatchInput(BATCH_COUNT, records))
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


def test_fold_window_run_main(benchmark):
    benchmark(lambda: run_main(flow))


def test_fold_window_cluster_main(benchmark):
    benchmark(lambda: cluster_main(flow, addresses=["localhost:9999"], proc_id=0))
