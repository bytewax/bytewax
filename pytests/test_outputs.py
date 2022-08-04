from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import StdOutputConfig, TestingOutputConfig


def test_capture():
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInputConfig(inp))

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_capture_multiple():
    flow = Dataflow()

    inp = ["a", "b"]
    flow.input("inp", TestingInputConfig(inp))

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
    flow.input("inp", TestingInputConfig(inp))

    flow.capture(StdOutputConfig())

    run_main(flow)

    captured = capfd.readouterr()
    assert captured.out == "a\nb\n"
