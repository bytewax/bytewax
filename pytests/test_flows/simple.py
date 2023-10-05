from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def get_flow(path):
    flow = Dataflow()
    inp = range(1000)
    flow.input("inp", TestingSource(inp))

    def mapper(item):
        return "ALL", f"{item}"

    flow.map("all_mapper", mapper)
    flow.output("out", FileSink(path))
    return flow
