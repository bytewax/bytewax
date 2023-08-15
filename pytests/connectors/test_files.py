from pathlib import Path

from bytewax.connectors.files import (
    CSVInput,
    DirInput,
    DirOutput,
    FileInput,
    FileOutput,
)
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput, run_main
from pytest import raises


def test_dir_input():
    flow = Dataflow()

    flow.input("inp", DirInput(Path("examples/sample_data/cluster")))

    out = []
    flow.output("out", TestingOutput(out))

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
    file_path = Path("examples/sample_data/cluster/partition-1.txt")

    flow = Dataflow()

    flow.input("inp", FileInput(file_path))

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert out == [
        "one1",
        "one2",
        "one3",
        "one4",
        "one5",
        "one6",
    ]


def test_csv_file_input():
    file_path = Path("examples/sample_data/metrics.csv")

    flow = Dataflow()

    flow.input("inp", CSVInput(file_path))

    out = []
    flow.output("out", TestingOutput(out))

    run_main(flow)

    assert out == [
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "0.132",
            "instance": "24ae8d",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "0.066",
            "instance": "c6585a",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "42.652",
            "instance": "ac20cd",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "51.846",
            "instance": "5f5533",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "2.296",
            "instance": "fe7f93",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "1.732",
            "instance": "53ea38",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "91.958",
            "instance": "825cc2",
        },
        {
            "index": "0",
            "timestamp": "2022-02-24 11:42:08",
            "value": "0.068",
            "instance": "77c1ca",
        },
    ]


def test_file_input_resume_state():
    file_path = Path("examples/sample_data/cluster/partition-1.txt")
    inp = FileInput(file_path)
    part = inp.build_part(str(file_path), None)
    assert part.next_batch() == ["one1"]
    assert part.next_batch() == ["one2"]
    resume_state = part.snapshot()
    assert part.next_batch() == ["one3"]
    assert part.next_batch() == ["one4"]
    part.close()

    inp = FileInput(file_path)
    part = inp.build_part(str(file_path), resume_state)
    assert part.snapshot() == resume_state
    assert part.next_batch() == ["one3"]
    assert part.next_batch() == ["one4"]
    assert part.next_batch() == ["one5"]
    assert part.next_batch() == ["one6"]
    with raises(StopIteration):
        part.next_batch()
    part.close()


def test_file_output(tmp_path):
    file_path = tmp_path / "out.txt"

    flow = Dataflow()

    inp = [
        ("1", "1"),
        ("2", "2"),
        ("3", "3"),
    ]
    flow.input("inp", TestingInput(inp))

    flow.output("out", FileOutput(file_path))

    run_main(flow)

    with open(file_path, "r") as f:
        out = f.readlines()
        assert out == [
            "1\n",
            "2\n",
            "3\n",
        ]


def test_dir_output(tmp_path):
    flow = Dataflow()

    inp = [
        ("0", "0"),
        ("1", "1"),
        ("2", "2"),
    ]
    flow.input("inp", TestingInput(inp))

    # Route each item to the partition index that is int version of
    # the key (which must be a str).
    flow.output("out", DirOutput(tmp_path, 3, assign_file=int))

    run_main(flow)

    with open(tmp_path / "part_0", "r") as f:
        out = f.readlines()
        assert out == ["0\n"]

    with open(tmp_path / "part_1", "r") as f:
        out = f.readlines()
        assert out == ["1\n"]

    with open(tmp_path / "part_2", "r") as f:
        out = f.readlines()
        assert out == ["2\n"]


def test_file_output_resume_state(tmp_path):
    file_path = tmp_path / "out.txt"

    out = FileOutput(file_path)
    part = out.build_part(str(file_path), None)
    part.write_batch(["one1"])
    part.write_batch(["one2"])
    part.write_batch(["one3"])
    resume_state = part.snapshot()
    part.write_batch(["one4"])
    part.close()

    out = FileOutput(file_path)
    part = out.build_part(str(file_path), resume_state)
    assert part.snapshot() == resume_state
    part.write_batch(["two4"])
    part.write_batch(["two5"])
    part.close()

    with open(file_path, "rt") as f:
        found = f.readlines()
        expected = [
            "one1\n",
            "one2\n",
            "one3\n",
            "two4\n",
            "two5\n",
        ]
        assert found == expected
