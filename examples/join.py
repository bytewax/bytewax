from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("join")


def key_getter(x):
    return str(x["user_id"])


inp1 = op.input("inp1", flow, TestingSource([{"user_id": 123, "name": "Bumble"}]))
k_inp1 = op.key_on("k1", inp1, key_getter)
name = op.map_value("name", k_inp1, lambda x: x["name"])
inp2 = op.input(
    "inp2", flow, TestingSource([{"user_id": 123, "email": "bee@bytewax.com"}])
)
k_inp2 = op.key_on("k2", inp2, key_getter)
email = op.map_value("email", k_inp2, lambda x: x["email"])
inp3 = op.input(
    "inp3", flow, TestingSource([{"user_id": 123, "color": "yellow", "sound": "buzz"}])
)
k_inp3 = op.key_on("k3", inp3, key_getter)
color = op.map_value("color", k_inp3, lambda x: x["color"])
sound = op.map_value("sound", k_inp3, lambda x: x["sound"])

joined = op.join("j1", name, email, color, sound)
op.output("out", joined, StdOutSink())
