import os
import signal
import subprocess
import tempfile

from pytest import raises, mark

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput


def test_run(entry_point, out):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))
    flow.map(lambda x: x + 1)
    flow.output("out", TestingOutput(out))

    entry_point(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_reraises_exception(entry_point):
    out = []
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))

    def boom(item):
        if item == 0:
            raise RuntimeError("BOOM")
        else:
            return item

    flow.map(boom)
    flow.output("out", TestingOutput(out))

    with raises(RuntimeError):
        entry_point(flow)

    assert len(out) < 3



@mark.skipif(
    os.name == "nt",
    reason=(
            "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
            " processes on this console so interrupts pytest itself"
    ),
)
def test_cluster_can_be_ctrl_c():
    """Test that we can stop cluster execution by sending SIGINT (ctrl+c)."""
    # Create a tmp file we can use to check the output
    tmp_file = tempfile.NamedTemporaryFile()
    # The dataflow we want to run is in ./test_flows/simple.py
    flow_path = f"pytests.test_flows.simple:get_flow('{tmp_file.name}')"
    process = subprocess.Popen(
        [
            "python",
            "-m",
            "bytewax.run",
            flow_path,
            # Spawn 2 processes
            "-p",
            "2",
            # With 2 workers per process
            "-w",
            "2",
            # Set epoch interval to 0 so that the output
            # is written to the file as soon as possible
            "--epoch-interval",
            "0",
        ],
        # Use PIPE to check the content of stdout
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Now wait for the file to contain at least 10 lines
    output = b""
    while len(output.splitlines()) < 10:
        tmp_file.seek(0)
        output = tmp_file.read()
    # And stop the dataflow by sending SIGINT (like ctrl+c)
    process.send_signal(signal.SIGINT)
    # Process termination should be handled properly
    stdout, stderr = process.communicate()
    assert b"KeyboardInterrupt:" in stderr
    # The file should not contain all the lines since we stopped it
    assert len(output.splitlines()) < 999
    # Close and delete the file
    tmp_file.close()
