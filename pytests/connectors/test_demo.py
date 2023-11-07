from datetime import timedelta

from bytewax.connectors.demo import RandomMetricSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, run_main


def test_random_metric_source():
    out = []

    flow = Dataflow("test_df")
    s = flow.input(
        "inp",
        RandomMetricSource(
            "volts", interval=timedelta(seconds=0), count=3, next_random=lambda: 42
        ),
    )
    s.output("out", TestingSink(out))

    run_main(flow)
    assert out == [("volts", 42), ("volts", 42), ("volts", 42)]
