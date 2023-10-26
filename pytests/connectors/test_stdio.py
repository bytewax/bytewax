from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main


def test_std_output(capfd):
    flow = Dataflow("test_df")

    inp = ["a", "b"]
    flow.input("inp", TestingSource(inp)).output("out", StdOutSink())

    run_main(flow)

    captured = capfd.readouterr()
    assert captured.out == "a\nb\n"
