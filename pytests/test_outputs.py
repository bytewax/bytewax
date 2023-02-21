from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import StdOutputConfig, TestingOutputConfig
from bytewax.testing import TestingInput


def test_capture():
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInput(inp))

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_capture_multiple():
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInput(inp))

    out1 = []
    flow.capture(TestingOutputConfig(out1))

    flow.map(str.upper)

    out2 = []
    flow.capture(TestingOutputConfig(out2))

    run_main(flow)

    assert sorted(out1) == sorted(["a", "b"])
    assert sorted(out2) == sorted(["A", "B"])


def test_std_output(capfd):
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInput(inp))

    flow.capture(StdOutputConfig())

    run_main(flow)

    captured = capfd.readouterr()
    assert captured.out == "a\nb\n"
