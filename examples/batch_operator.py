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

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingPeriodicInput


def add_key(x):
    return ("ALL", x)


def calc_avg(key__batch):
    key, batch = key__batch
    return sum(batch) / len(batch)


flow = Dataflow()
# Emit 20 items, with a key for the stateful operator
# and a 0.25 seconds timeout, so ~4 items per second
flow.input(
    "in",
    TestingPeriodicInput(timedelta(seconds=0.25), limit=20, cb=add_key),
)

# Batch for either 3 elements, or 1 second. This should emit at
# the size limit since we emit more than 3 items per second.
flow.batch("batch", size=3, timeout=timedelta(seconds=1))
flow.inspect(lambda x: print(f"Batch:\t\t{x[1]}"))

# Do some operation on the whole batch
flow.map(calc_avg)
flow.inspect(lambda x: print(f"Avg:\t\t{x}"))

# Now batch for either 10 elements or 1 second.
# This time, the batching should happen at the time limit,
# since we don't emit items fast enough to fill the size
# before the timeout triggers.
flow.map(add_key)
flow.batch("batch", size=10, timeout=timedelta(seconds=1))
flow.map(lambda x: f"Avg batch:\t{x[1]}")
flow.output("out", StdOutput())
