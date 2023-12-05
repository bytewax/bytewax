from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")


def key_getter(x):
    return str(x["user_id"])


inp1 = op.input("inp1", flow, TestingSource([{"user_id": 123, "name": "Bumble"}]))
(k_inp1,) = op.key_split("k1", inp1, key_getter, lambda x: x["name"])
inp2 = op.input(
    "inp2", flow, TestingSource([{"user_id": 123, "email": "bee@bytewax.com"}])
)
(k_inp2,) = op.key_split("k2", inp2, key_getter, lambda x: x["email"])
inp3 = op.input(
    "inp3", flow, TestingSource([{"user_id": 123, "color": "yellow", "sound": "buzz"}])
)
k_inp3, k_inp4 = op.key_split(
    "k3", inp3, key_getter, lambda x: x["color"], lambda x: x["sound"]
)

joined = op.join("j1", k_inp1, k_inp2, k_inp3, k_inp4)
op.output("out", joined, StdOutSink())
