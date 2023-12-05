import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def get_flow(path):
    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(range(1000)))
    s = op.key_on("key", s, lambda _x: "ALL")
    s = op.map_value("str", s, str)
    op.output("out", s, FileSink(path))

    return flow
