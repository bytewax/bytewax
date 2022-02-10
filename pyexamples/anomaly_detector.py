import random

import bytewax
from bytewax import inp


def random_datapoints():
    for _ in range(20):
        yield "QPS", random.randrange(0, 10)


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


def inspector(metric__value_mu_sigma_anomalous):
    metric, (value, mu, sigma, is_anomalous) = metric__value_mu_sigma_anomalous
    print(
        f"{metric}: value = {value}, mu = {mu:.2f}, sigma = {sigma:.2f}, {is_anomalous}"
    )


ec = bytewax.Executor()
flow = ec.Dataflow(inp.fully_ordered(random_datapoints()))
# ("metric", value)
flow.stateful_map(lambda: ZTestDetector(2.0), ZTestDetector.push)
# ("metric", (value, mu, sigma, is_anomalous))
flow.inspect(inspector)


if __name__ == "__main__":
    ec.build_and_run()
