"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

from examples.errors import working

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput


def broken_map(x):
    # XXX: Error here
    raise TypeError("A vague error message")


flow = Dataflow()
flow.input("inp", working.NumberInput(10))
flow.map(broken_map)
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
