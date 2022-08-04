from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import distribute, ManualInputConfig
from bytewax.outputs import TestingOutputConfig


def test_manual_input_config():
    flow = Dataflow()

    def input_builder(worker_index, worker_count, resume_state):
        assert resume_state is None
        yield (None, 1)
        yield
        yield (None, 2)
        yield
        yield (None, 3)

    flow.input("inp", ManualInputConfig(input_builder))

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
