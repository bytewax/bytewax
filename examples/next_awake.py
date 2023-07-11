from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.inputs import (
    DynamicInput,
    StatelessSource,
    PartitionedInput,
    StatefulSource,
)


class PeriodicSource(StatelessSource):
    def __init__(self, frequency):
        self.frequency = frequency
        self._next_awake = datetime.now(timezone.utc)
        self._i = 0

    def next_awake(self):
        return self._next_awake

    def next(self):
        self._i += 1
        if self._i >= 10:
            raise StopIteration()
        self._next_awake += self.frequency
        return ["VALUE"]


class PeriodicInput(DynamicInput):
    def __init__(self, frequency):
        self.frequency = frequency

    def build(self, worker_index, worker_count):
        return PeriodicSource(frequency=self.frequency)


stateless_flow = Dataflow()
stateless_flow.input("periodic", PeriodicInput(timedelta(seconds=1)))
stateless_flow.output("stdout", StdOutput())


class StatefulPeriodicSource(StatefulSource):
    def __init__(self, frequency, next_awake):
        self.frequency = frequency
        self._next_awake = next_awake
        self._i = 0

    def next(self):
        self._i += 1
        if self._i >= 10:
            raise StopIteration()
        self._next_awake += self.frequency
        return ["VALUE"]

    def snapshot(self):
        return {
            "frequency": self.frequency,
            "_next_awake": self._next_awake.isoformat(),
        }

    def next_awake(self):
        return self._next_awake


class PeriodicPartitionedInput(PartitionedInput):
    def __init__(self, frequency):
        self.frequency = frequency

    def list_parts(self):
        return {"singleton"}

    def build_part(self, for_part, resume_state):
        assert for_part == "singleton"
        resume_state = resume_state or {}
        frequency = resume_state.get("frequency", self.frequency)
        next_awake = datetime.fromisoformat(
            resume_state.get("_next_awake", datetime.now(timezone.utc).isoformat())
        )
        return StatefulPeriodicSource(frequency, next_awake)


stateful_flow = Dataflow()
stateful_flow.input("periodic", PeriodicPartitionedInput(timedelta(seconds=1)))
stateful_flow.output("stdout", StdOutput())
