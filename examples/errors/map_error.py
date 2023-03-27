"""
This dataflow crashes for an Exception in the mapper function
of the `map` operator.
We cause the crash by raising an exception in one of the maps.
"""

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import StatelessSource, DynamicInput


class NumberSource(StatelessSource):
    def __init__(self, iterator):
        self.iterator = iterator

    def next(self):
        return next(self.iterator)

    def close(self):
        pass


class NumberInput(DynamicInput):
    def __init__(self, max):
        self.max = max
        self.iterator = iter(range(max))

    def build(self, worker_index, worker_count):
        # This will duplicate data in each
        # process but not in each worker
        # since they share the iterator
        return NumberSource(self.iterator)


def broken_map(x):
    # XXX: Error here
    raise TypeError("A vague error message")


flow = Dataflow()
flow.input("inp", NumberInput(10))
flow.map(broken_map)
flow.output("out", StdOutput())


if __name__ == "__main__":
    from bytewax.execution import run_main

    run_main(flow)
