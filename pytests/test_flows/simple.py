import time

import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def _throttle(item):
    """Slow each item so the flow outlives a SIGINT sent after its first output.

    Unthrottled, the 1000 items finish in about 0.1 seconds, which a
    test under load can lose to.

    """
    time.sleep(0.02)
    return item


def get_flow(path):
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(range(1000)))
    s = op.map("throttle", s, _throttle)
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.map_value("str", s, str)
    op.output("out", s, FileSink(path))

    return flow
