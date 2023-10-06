"""
This dataflow crashes because we never add an output to it.
"""

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", TestingSource(range(10)))
flow.map(stringify)
# XXX: Error here
# flow.output("out", StdOutSink())
