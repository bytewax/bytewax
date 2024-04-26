import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource
from pytest import mark, raises


def test_run(entry_point):
    flow = Dataflow("test_df")
    inp = range(3)
    stream = op.input("inp", flow, TestingSource(inp))
    stream = op.map("add_one", stream, lambda x: x + 1)
    out = []
    op.output("out", stream, TestingSink(out))

    entry_point(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_reraises_exception(entry_point):
    flow = Dataflow("test_df")
    inp = range(3)
    stream = op.input("inp", flow, TestingSource(inp))

    def boom(item):
        if item == 0:
            msg = "BOOM"
            raise RuntimeError(msg)
        else:
            return item

    stream = op.map("explode", stream, boom)
    out = []
    op.output("out", stream, TestingSink(out))

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
def test_cluster_can_be_ctrl_c(tmp_path):
    """Test that we can stop cluster execution by sending SIGINT (ctrl+c)."""
    tmp_path = tmp_path / "out.txt"

    with open(tmp_path, "w+b") as tmp_file:
        # The dataflow we want to run is in ./test_flows/simple.py
        flow_path = f"pytests.test_flows.simple:get_flow('{tmp_file.name}')"
        cmd = [
            # Ensure that we use the exact same Python interpreter as
            # here; might be in a venv.
            sys.executable,
            "-m",
            "bytewax.testing",
            flow_path,
            # Spawn 2 processes
            "-p",
            "2",
            # With 2 workers per process
            "-w",
            "2",
            # Set snapshot interval to 0 so that the output is written
            # to the file as soon as possible
            "-s",
            "0",
        ]
        process = subprocess.Popen(
            cmd,
            # Use PIPE to check the content of stdout
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            # Wait for the file to contain at least a line to show the
            # dataflow has started.
            output = b""
            timeout_at = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
            while len(output.splitlines()) < 1:
                if datetime.now(tz=timezone.utc) >= timeout_at:
                    msg = "# dataflow didn't write output in time"
                    raise subprocess.TimeoutExpired(msg, 5)
                process.poll()
                if process.returncode is not None:
                    msg = "# dataflow exited too quickly"
                    raise subprocess.TimeoutExpired(msg, 0)

                tmp_file.seek(0)
                output = tmp_file.read()

            # And stop the dataflow by sending SIGINT (like ctrl+c)
            process.send_signal(signal.SIGINT)

            # Process termination should be handled properly
            stdout, stderr = process.communicate(timeout=5)
            assert b"KeyboardInterrupt:" in stderr

            # The file should not contain all the lines since we stopped it
            tmp_file.seek(0)
            output = tmp_file.read()
            assert len(output.splitlines()) < 999
        except subprocess.TimeoutExpired as ex:
            process.kill()
            stdout, stderr = process.communicate()
            raise subprocess.CalledProcessError(
                process.returncode, cmd, stdout, stderr
            ) from ex
