from bytewax.connectors.files import FileOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput


def get_flow(path):
    flow = Dataflow()
    inp = range(1000)
    flow.input("inp", TestingInput(inp))

    def mapper(item):
        return "ALL", f"{item}"

    flow.map(mapper)
    flow.output("out", FileOutput(path))
    return flow
