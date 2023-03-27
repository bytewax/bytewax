from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.testing import TestingInput


def double(x):
    import time
    time.sleep(0.1)
    return x * 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow()
flow.input("inp", TestingInput(range(20)))
flow.map(double)
flow.map(minus_one)
flow.map(stringy)
flow.output("out", StdOutput())
