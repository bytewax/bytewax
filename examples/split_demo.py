from dataclasses import dataclass
from datetime import timedelta
from random import choice
from typing import Dict

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource


@dataclass
class Msg:
    key: str
    val: str
    headers: Dict[str, int]
    num: int


class MsgSource(SimplePollingSource):
    def __init__(self):
        super().__init__(interval=timedelta(seconds=1))
        self.keys = ["a", "b", "c"]
        self.nums = [1, 2, 3]

    def next_item(self):
        key = choice(self.keys)
        value = f"{key}_value"
        headers = {"key": 1}
        num = choice(self.nums)
        return Msg(key, value, headers, num)


flow = Dataflow("my_flow")
inp = op.input("inp", flow, MsgSource())

vals = op.map("vals", inp, lambda msg: (msg.key, msg.val))
op.inspect("v", vals)
headers = op.map("headers", inp, lambda msg: (msg.key, msg.headers))
op.inspect("h", headers)
nums = op.map("nums", inp, lambda msg: (msg.key, msg.num))
op.inspect("n", nums)

tog = op.join_named("join", vals=vals, headers=headers, nums=nums)
op.output("tog_out", tog, StdOutSink())
