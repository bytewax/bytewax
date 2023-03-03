from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput


def test_std_output(capfd):
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInput(inp))

    flow.output("out", StdOutput())

    run_main(flow)

    captured = capfd.readouterr()
    assert captured.out == "a\nb\n"
