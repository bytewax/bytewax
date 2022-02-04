import random

from bytewax import Executor


def random_datapoints():
    for _ in range(20):
        yield (1, random.randrange(0, 10))


class ZTestDetector:
    """Anomaly detector.

    Use with a call to flow.map().

    Looks at how many standard deviations the current item is away
    from the mean (Z-score) of the last 10 items. Mark as anomalous if
    over the threshold specified.
    """
    def __init__(self, threshold_z):
        self.threshold_z = threshold_z
        
        self.last_10 = []
        self.mu = None
        self.sigma = None

    def _push(self, x):
        self.last_10.insert(0, x)
        del self.last_10[10:]

    def _recalc_stats(self):
        last_len = len(self.last_10)
        self.mu = sum(self.last_10) / last_len
        sigma_sq = sum((x - self.mu) ** 2 for x in self.last_10) / last_len
        self.sigma = sigma_sq ** 0.5
        
    def __call__(self, x):
        is_anomalous = False
        if self.mu and self.sigma:
            is_anomalous = abs(x - self.mu) / self.sigma > self.threshold_z

        self._push(x)
        self._recalc_stats()
        
        return (x, self.mu, self.sigma, is_anomalous)


def inspector(x_mu_sigma_anomalous):
    x, mu, sigma, is_anomalous = x_mu_sigma_anomalous
    print(f"x = {x}, mu = {mu:.2f}, sigma = {sigma:.2f}, {is_anomalous}")
    

ec = Executor()
flow = ec.Dataflow(random_datapoints())
# Think about what semantics you'd want with multiple workers / epochs.
flow.map(ZTestDetector(2.0))
flow.inspect(inspector)


if __name__ == "__main__":
    ec.build_and_run()
