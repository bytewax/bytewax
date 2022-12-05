import random

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig


def input_builder(worker_index, worker_count, resume_state):
    def random_datapoints():
        for _ in range(20):
            yield None, ("QPS", random.randrange(0, 10))

    return random_datapoints()


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
        print(self)

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


if __name__ == "__main__":
    flow = Dataflow()
    flow.input("input", ManualInputConfig(input_builder))
    # ("metric", value)
    flow.stateful_map("AnomalyDetector", lambda: ZTestDetector(2.0), ZTestDetector.push)
    # ("metric", (value, mu, sigma, is_anomalous))
    flow.capture(ManualOutputConfig(output_builder))
    spawn_cluster(flow)
