import random
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.windowing as w
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.windowing import EventClock, TumblingWindower

BATCH_SIZE = 100_000
BATCH_COUNT = 10

align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)

inp = [align_to + timedelta(seconds=i) for i in range(BATCH_SIZE)]

clock_config = EventClock(
    ts_getter=lambda x: x,
    wait_for_system_duration=timedelta(seconds=0),
)

windower = TumblingWindower(align_to=align_to, length=timedelta(minutes=1))


def add(acc, x):
    acc.append(x)
    return acc


flow = Dataflow("bench")
wo = (
    op.input("in", flow, TestingSource(inp, BATCH_COUNT))
    .then(op.key_on, "key-on", lambda _: str(random.randrange(0, 2)))
    .then(w.fold_window, "fold-window", clock_config, windower, list, add, list.__add__)
)
flat = op.flat_map("flatten-window", wo.down, lambda xs: (y for y in xs))
filtered_out = op.filter("filter_all", flat, lambda _x: False)
op.output("stdout", filtered_out, StdOutSink())
