import random

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.inputs import PartitionedInput, StatefulSource


class RandomMetricSource(StatefulSource):
    def __init__(self):
        self.iterator = iter(list(range(20)))

    def next(self):
        next(self.iterator)
        return "ALL", random.randrange(0, 10)

    def snapshot(self):
        return None

    def close(self):
        pass


class RandomMetricInput(PartitionedInput):
    def list_parts(self):
        return {"singleton"}

    def build_part(self, for_part, resume_state):
        assert for_part == "singleton"
        assert resume_state is None
        return RandomMetricSource()


class ZTestDetector:
    """Anomaly detector.

    Use with a call to flow.stateful_map().

    Looks at how many standard deviations the current item is away
    from the mean (Z-score) of the last 10 items. Mark as anomalous if
    over the threshold specified.
    """

    def __init__(self, threshold_z):
        self.threshold_z = threshold_z

        self.last_10 = []
        self.mu = None
        self.sigma = None

    def _push(self, value):
        self.last_10.insert(0, value)
        del self.last_10[10:]

    def _recalc_stats(self):
        last_len = len(self.last_10)
        self.mu = sum(self.last_10) / last_len
        sigma_sq = sum((value - self.mu) ** 2 for value in self.last_10) / last_len
        self.sigma = sigma_sq**0.5

    def push(self, value):
        is_anomalous = False
        if self.mu and self.sigma:
            is_anomalous = abs(value - self.mu) / self.sigma > self.threshold_z

        self._push(value)
        self._recalc_stats()
        return self, (value, self.mu, self.sigma, is_anomalous)


def output_builder(worker_index, worker_count):
    def inspector(input):
        metric, (value, mu, sigma, is_anomalous) = input
        print(
            f"{metric}: "
            f"value = {value}, "
            f"mu = {mu:.2f}, "
            f"sigma = {sigma:.2f}, "
            f"{is_anomalous}"
        )

    return inspector


flow = Dataflow()
flow.input("inp", RandomMetricInput())
# ("metric", value)
flow.stateful_map("AnomalyDetector", lambda: ZTestDetector(2.0), ZTestDetector.push)
# ("metric", (value, mu, sigma, is_anomalous))
flow.output("output", StdOutput())
