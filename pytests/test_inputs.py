from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import distribute, ManualInputConfig
from bytewax.outputs import TestingOutputConfig


def test_manual_input_config():
    def input_builder(worker_index, worker_count):
        yield 1
        yield
        yield 2
        yield
        yield 3

    flow = Dataflow(ManualInputConfig(input_builder))
    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_distribute():
    inp = ["a", "b", "c"]

    out1 = distribute(inp, 0, 2)

    assert list(out1) == ["a", "c"]

    out2 = distribute(inp, 1, 2)

    assert list(out2) == ["b"]
