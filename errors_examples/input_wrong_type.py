"""
This dataflow crashes when building the input source.
We cause the crash by returning None in DynamicInput.build.
"""

# Import functions that we don't need to break form the working example
import working

from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import StatelessSource, DynamicInput
from bytewax.window import TumblingWindow, SystemClockConfig, SessionWindow


class NumberSource(StatelessSource):
    def __init__(self, max, worker_index):
        if worker_index == 0:
            self.iterator = iter(range(max))

    def next(self):
        if self.iterator is not None:
            return next(self.iterator)

    def close(self):
        pass


class NumberInput(DynamicInput):
    def __init__(self, max):
        self.max = max

    def build(self, worker_index, worker_count):
        # XXX: Error here
        return None
        # Should be:
        # return NumberSource(self.max, worker_index)


flow = Dataflow()
flow.input("inp", NumberInput(10))
# Stateless operators
flow.filter(working.filter_op)
flow.filter_map(working.filter_map_op)
flow.flat_map(working.flat_map_op)
flow.inspect(working.inspect_op)
flow.inspect_epoch(working.inspect_epoch_op)
flow.map(working.map_op)
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
