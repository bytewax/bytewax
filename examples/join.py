from datetime import datetime, timedelta, timezone

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow
from bytewax.testing import TestingSource

clock = SystemClockConfig()
windower = TumblingWindow(
    length=timedelta(seconds=2), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)


flow = Dataflow("join")
inp1 = op.input("inp1", flow, TestingSource(["a"]))
k_inp1 = op.key_on("k1", inp1, lambda x: "KEY")
inp2 = op.input("inp2", flow, TestingSource(["b"]))
k_inp2 = op.key_on("k2", inp2, lambda x: "KEY")
inp3 = op.input("inp3", flow, TestingSource(["c"]))
k_inp3 = op.key_on("k3", inp3, lambda x: "KEY")

joined = op.join("j1", k_inp1, k_inp2, k_inp3)
op.output("out", joined, StdOutSink())
