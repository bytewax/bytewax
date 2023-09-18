from dataclasses import dataclass, field
from typing import List, Optional

from bytewax.connectors.demo import RandomMetricSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.operators import UnaryLogic


@dataclass
class _State:
    last_10: List[float] = field(default_factory=list)
    mu: Optional[float] = None
    sigma: Optional[float] = None

    def push(self, value):
        self.last_10.insert(0, value)
        del self.last_10[10:]
        self.recalc_stats()

    def recalc_stats(self):
        last_len = len(self.last_10)
        self.mu = sum(self.last_10) / last_len
        sigma_sq = sum((value - self.mu) ** 2 for value in self.last_10) / last_len
        self.sigma = sigma_sq**0.5

    def is_anomalous(self, threshold_z, value):
        if self.mu and self.sigma:
            return abs(value - self.mu) / self.sigma > threshold_z

        return False


@dataclass
class ZTestDetector(UnaryLogic):
    """Anomaly detector.

    Looks at how many standard deviations the current item is away
    from the mean (Z-score) of the last 10 items. Mark as anomalous if
    over the threshold specified.
    """

    threshold_z: float
    state: _State

    @classmethod
    def param_builder(cls, threshold_z):
        def builder(resume_state):
            state = resume_state if resume_state is not None else _State()
            return cls(threshold_z, state)

        return builder

    def on_item(self, value):
        is_anomalous = self.state.is_anomalous(self.threshold_z, value)
        self.state.push(value)
        return [(value, self.state.mu, self.state.sigma, is_anomalous)]

    def on_eof(self):
        return []

    def is_complete(self):
        return False

    def snapshot(self):
        return self.state


def pretty_formatter(key_value):
    metric, (value, mu, sigma, is_anomalous) = key_value
    return (
        f"{metric}: "
        f"value = {value}, "
        f"mu = {mu:.2f}, "
        f"sigma = {sigma:.2f}, "
        f"{is_anomalous}"
    )


flow = Dataflow("anomaly_detector")
metric1 = flow.input("inp_v", RandomMetricSource("v_metric"))
metric2 = flow.input("inp_hz", RandomMetricSource("hz_metric"))
metrics = metric1.merge("merge", metric2).assert_keyed("key")
# ("metric", value)
labeled_metrics = metrics.unary("detector", ZTestDetector.param_builder(2.0))
# ("metric", (value, mu, sigma, is_anomalous))
labeled_metrics.map("format", pretty_formatter).output("output", StdOutSink())
