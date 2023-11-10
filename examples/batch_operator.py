"""Showcase the use of the `batch` operator.

The operator can be used to batch items, with both a size limit
and a timeout.

`flow.batch` is a stateful operator, so you need to add a key to each
element for proper routing.

To show its abilities, we use an input that simulates a periodic source,
with events coming in at regular intervals, and see how batches work
with both the size and time limits.
"""
from datetime import timedelta

from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource


class CounterSource(SimplePollingSource):
    def __init__(self):
        super().__init__(interval=timedelta(seconds=0.25))
        self._it = iter(range(20))

    def next_item(self):
        return next(self._it)


def calc_avg(key__batch):
    key, batch = key__batch
    return sum(batch) / len(batch)


flow = Dataflow("batch")
# Emit 20 items, with a 0.25 seconds timeout, so ~4 items per second
stream = flow.input("in", CounterSource()).key_on("key", lambda _: "ALL")

# Batch for either 3 elements, or 1 second. This should emit at
# the size limit since we emit more than 3 items per second.
stream = stream.batch("batch_3_items", batch_size=3, timeout=timedelta(seconds=1))
stream.inspect("ins_batch")

# Do some operation on the whole batch
stream = stream.map("calc_avg", calc_avg)
stream.inspect("ins_avg")

# Now batch for either 10 elements or 1 second.
# This time, the batching should happen at the time limit,
# since we don't emit items fast enough to fill the size
# before the timeout triggers.
stream = stream.key_on("same_key", lambda _: "ALL")
stream = stream.batch("batch_avgs", batch_size=10, timeout=timedelta(seconds=1))
stream = stream.map("convert_to_string", lambda x: f"Avg batch:\t{x[1]}")
stream.output("out", StdOutSink())
