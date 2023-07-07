from random import random
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import DynamicInput, StatelessSource


class PeriodicSource(StatelessSource):
    def __init__(self, frequency):
        self._next_awake = datetime.now(timezone.utc)
        self.frequency = frequency

    def next(self):
        self._next_awake += self.frequency
        return [random()]

    def next_awake(self):
        return self._next_awake


class PeriodicInput(DynamicInput):
    def __init__(self, frequency):
        self.frequency = frequency

    def build(self, worker_index, worker_count):
        return PeriodicSource(frequency=self.frequency)


flow = Dataflow()
flow.input("periodic", PeriodicInput(timedelta(seconds=5)))
flow.output("stdout", StdOutput())
