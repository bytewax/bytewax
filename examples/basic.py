from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def double(x):
    return x * 2


def halve(x):
    return x // 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow("basic")

inp = flow.input("inp", TestingSource(range(10)))
evens, odds = inp.split("e_o", lambda x: x % 2 == 0)
combo = evens.map("halve", halve).merge("merge", odds.map("double", double))
combo.map("minus_one", minus_one).map("stringy", stringy).output("out", StdOutSink())
