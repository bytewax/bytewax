"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput


def broken_map(x, boh):
    # XXX: Error here
    return x["not here"]


flow = Dataflow()
flow.input("inp", TestingInput(range(10)))
flow.map(lambda x: ("ALL", x))
flow.stateful_map("broken", lambda: {}, broken_map)
flow.output("out", StdOutput())
