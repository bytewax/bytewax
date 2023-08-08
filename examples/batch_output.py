from datetime import datetime, timezone, timedelta

from bytewax.dataflow import Dataflow
from bytewax.outputs import (
    StatelessSink,
    DynamicOutput,
    PartitionedOutput,
    StatefulSink,
)
from bytewax.inputs import (
    StatelessSource,
    DynamicInput,
    PartitionedInput,
    StatefulSource,
)
from bytewax.connectors.stdio import StdOutput


class Source(StatefulSource):
    def __init__(self, name, counter):
        self._name = name
        self._counter = counter
        self._next_awake = datetime.now(timezone.utc)

    def next_batch(self):
        self._next_awake += timedelta(seconds=0.1)
        self._counter += 1
        return [(f"{self._counter % 5}", self._counter)]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return {"counter": self._counter}


class In(PartitionedInput):
    def list_parts(self):
        return ["1", "2", "3", "4", "5"]

    def build_part(self, for_part, resume_state):
        resume_state = resume_state or {"counter": 0}
        return Source(for_part, resume_state["counter"])


class Sink(StatefulSink):
    def __init__(self, name):
        self._name = name

    def write_batch(self, values):
        print(f"Values from {self._name}: {[i[1] for i in values]}")
        return values


class Out(PartitionedOutput):
    def list_parts(self):
        return ["1", "2", "3", "4", "5"]

    def build_part(self, for_part, resume_state):
        return Sink(for_part)


flow = Dataflow()
flow.input("in", In())
flow.output("out", Out(), min_batch_size=10, timeout=timedelta(seconds=2.0))
