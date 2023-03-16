"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

# Import functions that we don't need to break form the working example
import working
from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.window import TumblingWindow, SystemClockConfig, SessionWindow


def broken_map(x):
    raise TypeError("A really nice TypeError")


flow = Dataflow()
flow.input("inp", working.NumberInput(10))
# Stateless operators
flow.filter(working.filter_op)
flow.filter_map(working.filter_map_op)
flow.flat_map(working.flat_map_op)
flow.inspect(working.inspect_op)
flow.inspect_epoch(working.inspect_epoch_op)
flow.map(broken_map)
# Stateful operators
flow.reduce("reduce", working.reduce_op, working.reduce_is_complete)
cc = SystemClockConfig()
wc = TumblingWindow(length=timedelta(seconds=1))
flow.fold_window("fold_window", cc, wc, working.folder_builder, working.folder_op)
wc = SessionWindow(gap=timedelta(seconds=1))
flow.reduce_window("reduce_window", cc, wc, working.reduce_window_op)
flow.stateful_map("stateful_map", working.stateful_map_builder, working.stateful_map_op)
flow.map(lambda x: dict(x[1]))
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
