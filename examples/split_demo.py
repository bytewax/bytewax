from dataclasses import dataclass
from typing import Dict

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
inp = flow.input("inp", TestingSource([a, b, c]))
vals, headers, nums = inp.key_split(
    "fields",
    lambda msg: msg.key,
    lambda msg: msg.val,
    lambda msg: msg.headers,
    lambda msg: msg.num,
)
vals.inspect("v")
headers.inspect("h")
nums.inspect("n")
tog = flow.join_named("join", vals=vals, headers=headers, nums=nums)
tog.output("tog_out", StdOutSink())
