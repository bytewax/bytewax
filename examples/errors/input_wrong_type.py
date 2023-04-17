"""
This dataflow crashes when building the input source.
We cause the crash by returning None in DynamicInput.build.
"""
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput


class NumberInput(DynamicInput):
    def __init__(self, max):
        self.max = max

    def build(self, worker_index, worker_count):
        # XXX: Error here
        return None


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", NumberInput(10))
flow.map(stringify)
flow.output("out", StdOutput())
