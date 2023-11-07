from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def get_flow(path):
    flow = Dataflow("test_df")
    s = flow.input("inp", TestingSource(range(1000)))
    s = s.key_on("key", lambda _x: "ALL")
    s = s.map_value("str", str)
    s.output("out", FileSink(path))

    return flow
