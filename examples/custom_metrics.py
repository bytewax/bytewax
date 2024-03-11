from datetime import datetime, timedelta, timezone
from typing import Dict

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import (
    DynamicSource,
    StatelessSourcePartition,
)
from prometheus_client import Gauge

NEXT_BATCH_DELAY_GAUGE = Gauge(
    "next_batch_delay",
    "Calculated delay of when next batch was called in seconds",
    ["step_id", "worker_index"],
    unit="seconds",
)


class PeriodicPartition(StatelessSourcePartition):
    def __init__(self, step_id: str, metric_labels: Dict, frequency: timedelta):
        self.frequency = frequency
        self._next_awake = datetime.now(timezone.utc)
        self._counter = 0
        self._metric_labels = metric_labels

    def next_awake(self):
        return self._next_awake

    def next_batch(self):
        self._counter += 1
        # Calculate the delay between when this was supposed
        # to  be called, and when it is actually called
        delay = datetime.now(timezone.utc) - self._next_awake
        self._next_awake += self.frequency
        NEXT_BATCH_DELAY_GAUGE.labels(**self._metric_labels).set(delay.total_seconds())

        return [self._counter]


class PeriodicSource(DynamicSource):
    def __init__(self, frequency):
        self.frequency = frequency

    def build(self, step_id: str, worker_index: int, worker_count: int):
        metric_labels = {"step_id": step_id, "worker_index": worker_index}
        return PeriodicPartition(step_id, metric_labels, frequency=self.frequency)


flow = Dataflow("custom_metrics")
stream = op.input("periodic", flow, PeriodicSource(timedelta(seconds=1)))
op.output("stdout", stream, StdOutSink())
