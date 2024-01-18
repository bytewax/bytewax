from datetime import timedelta

import bytewax.operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, run_main


def test_resample():
    out = []
    flow = Dataflow("resample")
    s1 = op.input("s1", flow, RandomMetricSource("1", timedelta(seconds=0.1), 5))
    s2 = op.input("s2", flow, RandomMetricSource("2", timedelta(seconds=0.2), 4))
    s3 = op.input("s3", flow, RandomMetricSource("3", timedelta(seconds=0.5), 1))
    merged = op.merge("sources", s1, s2, s3)
    resampled = op.resample(
        "res", merged, timedelta(seconds=1.0), lambda p, batch: len(batch)
    )
    op.output("out", resampled, TestingSink(out))
    run_main(flow)
    assert out == [("1", 5), ("2", 4), ("3", 1)]
