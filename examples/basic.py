from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def double(x):
    return x * 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow()
flow.input("inp", TestingSource(range(10)))
flow.map(double)
flow.map(minus_one)
flow.map(stringy)
flow.output("out", StdOutSink())
