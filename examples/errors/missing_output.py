"""
This dataflow crashes because we never add an output to it.
"""

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", TestingInput(10))
flow.map(stringify)
# XXX: Error here
# flow.output("out", StdOutput())

if __name__ == "__main__":
    from bytewax.execution import run_main

    run_main(flow)
