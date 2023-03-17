"""
This dataflow crashes because we never add an output to it.
"""

# Import functions that we don't need to break form the working example
import working

from bytewax.dataflow import Dataflow


flow = Dataflow()
flow.input("inp", working.NumberInput(10))
flow.map(working.stringify)
# XXX: Error here
# flow.output("out", StdOutput())

if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
