# Example that uses [msgspec](https://jcristharif.com/msgspec/) in place of the
# default PickleSerde implementation. Install `msgspec` with `pip install msgspec`
# to run this example.
#
# USAGE: To see the effect of using a custom Serde implementation, start multiple
# processes by hand with `-i/-a`, or use the `bytewax.testing` runner to start multiple
# workers:
#
# `python -m bytewax.testing examples.msgspec_serde -p3 -w1`
import random
from datetime import datetime, timedelta, timezone
from typing import Any, override

import bytewax.operators as op
import bytewax.operators.window as w
import msgspec
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import EventClock, TumblingWindower
from bytewax.serde import Serde, set_serde_obj
from bytewax.testing import TestingSource

BATCH_SIZE = 100_000
BATCH_COUNT = 10

align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)


class TestData(msgspec.Struct, kw_only=True, tag="testdata"):
    timestamp: datetime


class MsgSpecSerde(Serde):
    """Serialize objects using `msgspec`."""

    def __init__(self):
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(type=TestData)

    @override
    def ser(self, obj: Any) -> bytes:
        return self._encoder.encode(obj)

    @override
    def de(self, s: bytes) -> TestData:
        return self._decoder.decode(s)


set_serde_obj(MsgSpecSerde())

inp = [TestData(timestamp=align_to + timedelta(seconds=1)) for i in range(BATCH_SIZE)]

clock_config = EventClock(
    ts_getter=lambda x: x.timestamp,
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
