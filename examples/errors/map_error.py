"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.testing import TestingInput


def broken_map(x):
    # XXX: Error here
    raise TypeError("A vague error message")


flow = Dataflow()
flow.input("inp", TestingInput(range(10)))
flow.map(broken_map)
flow.output("out", StdOutput())
