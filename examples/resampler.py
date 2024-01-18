from datetime import timedelta
from typing import List, Optional

from bytewax import operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow

flow = Dataflow("resampler")

s1 = op.input("source1", flow, RandomMetricSource("tmp1", timedelta(seconds=0.001)))
s2 = op.input("source2", flow, RandomMetricSource("tmp2", timedelta(seconds=0.04)))
s3 = op.input("source3", flow, RandomMetricSource("tmp3", timedelta(seconds=1.005)))
merged = op.merge("sources", s1, s2, s3)


def sampler(batch: List[float]) -> Optional[float]:
    if batch:
        return sum(batch) / len(batch)
    else:
        return None


resampled = op.resample("resample", merged, timedelta(seconds=0.5), sampler)
op.inspect("resmapled", resampled)
