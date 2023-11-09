from datetime import datetime, timedelta, timezone

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators.window import SystemClockConfig, TumblingWindow
from bytewax.testing import TestingSource

clock = SystemClockConfig()
windower = TumblingWindow(
    length=timedelta(seconds=2), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)


flow = Dataflow("join")
inp1 = flow.input("inp1", TestingSource(["a"])).key_on("k1", lambda x: "KEY")
inp2 = flow.input("inp2", TestingSource(["b"])).key_on("k2", lambda x: "KEY")
inp3 = flow.input("inp3", TestingSource(["c"])).key_on("k3", lambda x: "KEY")

inp1.join("j1", inp2).join("j2", inp3).output("out", StdOutSink())
