from datetime import timedelta

import bytewax.operators as op
from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, run_main


def test_random_metric_source():
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        RandomMetricSource(
            "volts", interval=timedelta(seconds=0), count=3, next_random=lambda: 42
        ),
    )
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("volts", 42), ("volts", 42), ("volts", 42)]
