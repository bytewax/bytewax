from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, run_main


def test_std_output(capfd):
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInput(inp))

    flow.output("out", StdOutput())

    run_main(flow)

    captured = capfd.readouterr()
    assert captured.out == "a\nb\n"
