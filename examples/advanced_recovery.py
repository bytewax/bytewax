"""Implement a custom sink and source to keep track of state.
TOOD: This should probably be turned into a test for recovery
      rather than an example before merging the advanced_recovery PR.
"""

from datetime import datetime, timedelta, timezone
from random import random
from typing import Dict, List, Optional, Tuple

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

# setup_tracing(log_level="TRACE")


class CountPartition(StatefulSourcePartition):
    def __init__(self, for_part, state, interval):
        self.counter = state or 0
        self.key = for_part
        self.interval = interval
        self._next_awake = datetime.now(tz=timezone.utc)

    def next_batch(self):
        self.counter += 1
        self._next_awake += self.interval
        return [{"part": self.key, "counter": self.counter, "value": random() * 100}]

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return self.counter


class CountInput(FixedPartitionedSource):
    def __init__(self, interval):
        self.interval = interval

    def list_parts(self):
        return ["input_1", "input_2", "input_3"]

    def build_part(self, step_id, for_part, resume_state):
        print(f"Input: building part {for_part} with state {resume_state}")
        print()
        return CountPartition(for_part, resume_state, self.interval)


def running_avg(state: Optional[Dict], data: Dict) -> Tuple[Dict, Dict]:
    # Random crash here
    if data["value"] >= 99:
        msg = "BOOM"
        raise ValueError(msg)

    new_value = data["value"]
    if state is None:
        state = {"avg": new_value, "values": [new_value]}
    else:
        state["values"].append(new_value)
        state["avg"] = sum(state["values"]) / len(state["values"])
    data["state"] = state
    return (state, data)


class TestPartition(StatefulSinkPartition):
    def __init__(self, part, counter):
        self.part = part
        self.counter = counter or 0

    def write_batch(self, values) -> None:
        for data in values:
            self.counter += 1
            stateful_batch_state = len(data["state"]["values"])
            input_part = data["part"]
            input_state = data["counter"]
            output_state = self.counter
            print(f"From input '{input_part}' to output '{self.part}':")
            print(f"  - Input state counter: {input_state}")
            print(f"  - StatefulBatch counter: {stateful_batch_state}")
            print(f"  - Output state counter: {output_state}")
            print()

    def snapshot(self):
        return self.counter

    def close(self) -> None:
        self.counter = 0


class TestOutput(FixedPartitionedSink):
    def list_parts(self) -> List[str]:
        return ["output_1", "output_2", "output_3", "output_4"]

    def build_part(self, step_id, for_part, resume_state):
        print(f"Output: building part {for_part} with state {resume_state}")
        print()
        return TestPartition(for_part, resume_state)


flow = Dataflow("advanced_recovery")
inp = op.input("input", flow, CountInput(timedelta(seconds=1.2)))
inp = op.key_on("key_on_part", inp, lambda x: x["part"])
res = op.stateful_map("running_avg", inp, running_avg)
op.output("out", res, TestOutput())
