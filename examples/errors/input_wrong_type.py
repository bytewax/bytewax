"""
This dataflow crashes when building the input source.
We cause the crash by returning None in DynamicInput.build.
"""
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource


class NumberSource(DynamicSource):
    def __init__(self, n):
        self._n = n

    def build(self, worker_index, worker_count):
        # XXX: Error here
        return None


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", NumberSource(10))
flow.map(stringify)
flow.output("out", StdOutSink())
