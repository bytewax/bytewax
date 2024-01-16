from dataclasses import dataclass
from typing import Dict

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


@dataclass
class Msg:
    key: str
    val: str
    headers: Dict[str, int]
    num: int


a = Msg("a_key", "a_val", {"a": 1}, 1)
b = Msg("b_key", "b_val", {"b": 1}, 2)
c = Msg("c_key", "c_val", {"c": 1}, 3)

flow = Dataflow("my_flow")
inp = op.input("inp", flow, TestingSource([a, b, c]))

vals = op.map("vals", inp, lambda msg: (msg.key, msg.val))
op.inspect("v", vals)
headers = op.map("headers", inp, lambda msg: (msg.key, msg.headers))
op.inspect("h", headers)
nums = op.map("nums", inp, lambda msg: (msg.key, msg.num))
op.inspect("n", nums)

tog = op.join_named("join", vals=vals, headers=headers, nums=nums)
op.output("tog_out", tog, StdOutSink())
