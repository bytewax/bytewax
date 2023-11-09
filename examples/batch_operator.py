"""Showcase the use of the `batch` operator.

The operator can be used to batch items, with both a size limit
and a timeout.

`flow.batch` is a stateful operator, so you need to add a key to each
element for proper routing.

To show its abilities, we use an input that simulates a periodic source,
with events coming in at regular intervals, and see how batches work
with both the size and time limits.
"""
from datetime import datetime, timedelta, timezone

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


class PeriodicPartition(StatefulSourcePartition):
    def __init__(self):
        self._counter = 0
        self._next_awake = datetime.now(timezone.utc)

    def next_batch(self, sched):
        res = self._counter
        self._counter += 1
        self._next_awake += timedelta(seconds=0.25)

        if self._counter >= 20:
            raise StopIteration()

        return [res]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return None


class PeriodicSource(FixedPartitionedSource):
    def list_parts(self):
        return ["singleton"]

    def build_part(self, now, for_part, resume_state):
        return PeriodicPartition()


def add_key(x):
    return ("ALL", x)


def calc_avg(key__batch):
    key, batch = key__batch
    return sum(batch) / len(batch)


flow = Dataflow("batch")
# Emit 20 items, with a 0.25 seconds timeout, so ~4 items per second
inp = flow.input("in", PeriodicSource())
# Add a key for the stateful operator
keyed = inp.map("add_key", add_key).key_assert("key")

# Batch for either 3 elements, or 1 second. This should emit at
# the size limit since we emit more than 3 items per second.
batched = keyed.batch("batch", batch_size=3, timeout=timedelta(seconds=1))
batched.inspect("inspect batched", lambda _, x: print(f"Batch:\t\t{x[1]}"))

# Do some operation on the whole batch
avg = batched.map("calc avg", calc_avg)
avg.inspect("inspect avg", lambda _, x: print(f"Avg:\t\t{x}"))

# Now batch for either 10 elements or 1 second.
# This time, the batching should happen at the time limit,
# since we don't emit items fast enough to fill the size
# before the timeout triggers.
keyed = avg.map("add key", add_key).key_assert("key2")
batch = keyed.batch("batch 2", batch_size=10, timeout=timedelta(seconds=1))
representation = batch.map("convert to string", lambda x: f"Avg batch:\t{x[1]}")
representation.output("out", StdOutSink())
