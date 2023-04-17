"""
This dataflow crashes because we never add an input operator.
"""

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow


def stringify(x):
    return f"{x}"


flow = Dataflow()
# XXX: Error here
# flow.input("inp", TestingInput(10))
flow.map(stringify)
flow.output("out", StdOutput())
