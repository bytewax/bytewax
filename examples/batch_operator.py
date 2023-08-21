from datetime import timedelta
from time import sleep

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput


def slow_iterator():
    for i in range(50):
        sleep(0.2)
        yield i


flow = Dataflow()
flow.input("in", TestingInput(slow_iterator()))
# Add key for stateful operator
flow.map(lambda x: ("ALL", x))
flow.batch("batch", size=10, timeout=timedelta(seconds=1))
# Remove key to allow dynamic output batching, otherwise
# the key will be seen as the first element of the batch,
# and the whole batch will be seen as the second element.
flow.map(lambda key__batch: key__batch[1])
flow.output("out", StdOutput(), batched_input=True)
