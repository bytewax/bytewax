"""
This dataflow crashes because we never add an input operator.
"""

import working

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput


flow = Dataflow()
# XXX: Error here
# flow.input("inp", working.NumberInput(10))
flow.map(working.stringify)
flow.output("out", StdOutput())

if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
