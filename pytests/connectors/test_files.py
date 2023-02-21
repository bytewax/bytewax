from pathlib import Path

from pytest import raises

from bytewax.connectors.files import DirInput, FileInput
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.outputs import TestingOutputConfig


def test_dir_input():
    flow = Dataflow()

    flow.input("inp", DirInput(Path("examples/sample_data/cluster")))

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert "one1" in out
    assert "two1" in out
    assert "three1" in out
    assert "four1" in out
    assert "five1" in out


def test_dir_input_raises_on_non_exist():
    path = Path("examples/sample_data/bluster")

    with raises(ValueError) as exinfo:
        flow = Dataflow()

        flow.input("inp", DirInput(path))

        run_main(flow)

    assert str(exinfo.value) == f"input directory `{path}` does not exist"


def test_dir_input_raises_on_file():
    path = Path("examples/sample_data/cluster/partition-1.txt")

    with raises(ValueError) as exinfo:
        flow = Dataflow()

        flow.input("inp", DirInput(path))

        run_main(flow)

    assert str(exinfo.value) == f"input directory `{path}` is not a directory"


def test_file_input():
    flow = Dataflow()

    flow.input("inp", FileInput("examples/sample_data/cluster/partition-1.txt"))

    out = []
    flow.capture(TestingOutputConfig(out))

    run_main(flow)

    assert out == [
        "one1",
        "one2",
        "one3",
        "one4",
        "one5",
        "one6",
    ]
