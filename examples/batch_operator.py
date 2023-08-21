from datetime import datetime, timedelta, timezone

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.outputs import PartitionedOutput, StatefulSink


class Source(StatelessSource):
    def __init__(self, worker_index):
        self._worker_index = worker_index
        self._counter = 0
        self._next_awake = datetime.now(timezone.utc)

    def next_batch(self):
        if self._worker_index != 0:
            raise StopIteration()

        self._next_awake += timedelta(seconds=0.2)
        self._counter += 1

        if self._counter >= 20:
            raise StopIteration()

        return [self._counter]

    def next_awake(self):
        return self._next_awake


class In(DynamicInput):
    def build(self, worker_index, worker_count):
        return Source(worker_index)


class Sink(StatefulSink):
    def __init__(self, name):
        self._name = name

    def write_batch(self, values):
        for i in values:
            print(f"Output part {self._name}: {i}")


class Out(PartitionedOutput):
    def list_parts(self):
        return ["1", "2", "3", "4", "5"]

    def build_part(self, for_part, resume_state):
        return Sink(for_part)


flow = Dataflow()
flow.input("in", In())
# Add key for stateful operator
flow.map(lambda x: ("ALL", x))
flow.batch("batch", size=10, timeout=timedelta(seconds=1))
flow.output("out", Out(), batched_input=True)

# Rebatch again after the output unpacked the items, use a
# dynamic output this time
flow.batch("batch", size=3, timeout=timedelta(seconds=1))
# XXX: To work with a dynamic output, we need to remove the key, otherwise the key
# will be seen as the first element of the batch, and the whole batch will be
# seen as the second element.
flow.map(lambda key__batch: key__batch[1])
# Map the items to show a different label. We have a batch here, so we need to
# trea it like such
flow.map(lambda batch: [f"Dynamic out: {i}" for i in batch])
flow.output("out", StdOutput(), batched_input=True)
