"""
This dataflow crashes for an Exception while getting the next input.
We cause the crash by raising an Exception in DynamicInput.next
"""
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput, StatelessSource


class NumberSource(StatelessSource):
    def __init__(self, max):
        self.iterator = iter(range(max))

    def next(self):
        # XXX: Error here
        raise ValueError("A vague error")
        return next(self.iterator)

    def close(self):
        pass


class NumberInput(DynamicInput):
    def __init__(self, max):
        self.max = max

    def build(self, worker_index, worker_count):
        return NumberSource(self.max)


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", NumberInput(10))
flow.map(stringify)
flow.output("out", StdOutput())
