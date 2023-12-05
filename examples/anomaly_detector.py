from dataclasses import dataclass, field
from typing import List, Optional

import bytewax.operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("anomaly_detector")
metric1 = op.input("inp_v", flow, RandomMetricSource("v_metric"))
metric2 = op.input("inp_hz", flow, RandomMetricSource("hz_metric"))
metrics = op.merge("merge", metric1, metric2)
# ("metric", value)


@dataclass
class DetectorState:
    last_10: List[float] = field(default_factory=list)
    mu: Optional[float] = None
    sigma: Optional[float] = None

    def push(self, value):
        self.last_10.insert(0, value)
        del self.last_10[10:]
        self._recalc_stats()

    def _recalc_stats(self):
        last_len = len(self.last_10)
        self.mu = sum(self.last_10) / last_len
        sigma_sq = sum((value - self.mu) ** 2 for value in self.last_10) / last_len
        self.sigma = sigma_sq**0.5

    def is_anomalous(self, value, threshold_z):
        if self.mu and self.sigma:
            return abs(value - self.mu) / self.sigma > threshold_z

        return False


def mapper(state, value):
    is_anomalous = state.is_anomalous(value, threshold_z=2.0)
    state.push(value)
    emit = (value, state.mu, state.sigma, is_anomalous)
    # Always return the state so it is never discarded.
    return (state, emit)


labeled_metrics = op.stateful_map("detector", metrics, DetectorState, mapper)
# ("metric", (value, mu, sigma, is_anomalous))


def pretty_formatter(key_value):
    metric, (value, mu, sigma, is_anomalous) = key_value
    return (
        f"{metric}: "
        f"value = {value}, "
        f"mu = {mu:.2f}, "
        f"sigma = {sigma:.2f}, "
        f"{is_anomalous}"
    )


lines = op.map("format", labeled_metrics, pretty_formatter)
op.output("output", lines, StdOutSink())
