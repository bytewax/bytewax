"""
This dataflow crashes for an Exception while getting the next input.
We cause the crash by raising an Exception in DynamicInput.next
"""

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class NumberPartition(StatefulSourcePartition):
    def __init__(self, n):
        self.iterator = iter(range(n))

    def next_batch(self):
        # XXX: Error here
        msg = "A vague error"
        raise ValueError(msg)
        return [next(self.iterator)]

    def snapshot(self):
        return None


class NumberSource(FixedPartitionedSource):
    def __init__(self, n):
        self._n = n

    def list_parts(self):
        return {"one"}

    def build_part(self, for_key, resume_state):
        return NumberPartition(self._n)


def stringify(x):
    return f"{x}"


flow = Dataflow()
flow.input("inp", NumberSource(10))
flow.map(stringify)
flow.output("out", StdOutSink())
