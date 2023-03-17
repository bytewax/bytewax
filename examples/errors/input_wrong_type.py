"""
This dataflow crashes when building the input source.
We cause the crash by returning None in DynamicInput.build.
"""

# Import functions that we don't need to break form the working example
import working

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import StatelessSource, DynamicInput


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
flow.map(working.stringify)
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
