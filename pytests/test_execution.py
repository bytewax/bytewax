import os
import signal
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import BinaryIO

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
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


def test_reraises_custom_exception(entry_point):
    class CustomException(Exception):
        """A custom exception with more than one argument"""

        def __init__(self, msg, b):
            self.msg = msg
            self.b = b

    flow = Dataflow("test_df")
    inp = range(3)
    stream = op.input("inp", flow, TestingSource(inp))

    def boom(item):
        if item == 0:
            msg = "BOOM"
            raise CustomException(msg, 1)
        else:
            return item

    stream = op.map("explode", stream, boom)
    out = []
    op.output("out", stream, TestingSink(out))

    with raises(BytewaxRuntimeError):
        with raises(CustomException):
            entry_point(flow)

    assert len(out) < 3


def _assert_can_be_ctrl_c(proc: subprocess.Popen, out_file: BinaryIO):
    try:
        # Wait for the file to contain at least a line to show the
        # dataflow has started.
        output = b""
        timeout_at = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
        while len(output.splitlines()) < 1:
            if datetime.now(tz=timezone.utc) >= timeout_at:
                msg = "dataflow didn't write output in time"
                raise TimeoutError(msg)
            proc.poll()
            if proc.returncode is not None:
                msg = "dataflow exited too quickly"
                raise RuntimeError(msg)

            out_file.seek(0)
            output = out_file.read()

        # And stop the dataflow by sending SIGINT (like ctrl+c)
        proc.send_signal(signal.SIGINT)

        # Process termination should be handled properly
        stdout, stderr = proc.communicate(timeout=5)
        assert b"KeyboardInterrupt" in stderr

        # The file should not contain all the lines since we stopped it
        out_file.seek(0)
        output = out_file.read()
        assert len(output.splitlines()) < 999
    except (subprocess.TimeoutExpired, TimeoutError, RuntimeError) as ex:
        proc.kill()
        stdout, stderr = proc.communicate()
        print("--- Captured STDOUT of subprocess ---")
        sys.stdout.buffer.write(stdout)
        print("--- Captured STDERR of subprocess ---")
        sys.stdout.buffer.write(stderr)
        print("-------------------------------------")
        raise subprocess.CalledProcessError(
            proc.returncode,
            proc.args,
            stdout,
            stderr,
        ) from ex


@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_single_worker_can_be_ctrl_c(tmp_path):
    tmp_path = tmp_path / "out.txt"

    with open(tmp_path, "w+b") as tmp_file:
        # The dataflow we want to run is in ./test_flows/simple.py
        flow_path = f"pytests.test_flows.simple:get_flow('{tmp_file.name}')"
        args = [
            # Ensure that we use the exact same Python interpreter as
            # here; might be in a venv.
            sys.executable,
            "-m",
            "bytewax.run",
            flow_path,
            # With 1 worker per process to ensure `run_main` is used.
            "-w",
            "1",
            # Set snapshot interval to 0 so that the output is written
            # to the file as soon as possible
            "-s",
            "0",
        ]
        proc = subprocess.Popen(
            args,
            # Use PIPE to check the content of stdout
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _assert_can_be_ctrl_c(proc, tmp_file)


@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_manual_cluster_can_be_ctrl_c(tmp_path):
    tmp_path = tmp_path / "out.txt"

    with open(tmp_path, "w+b") as tmp_file:
        # The dataflow we want to run is in ./test_flows/simple.py
        flow_path = f"pytests.test_flows.simple:get_flow('{tmp_file.name}')"
        args = [
            # Ensure that we use the exact same Python interpreter as
            # here; might be in a venv.
            sys.executable,
            "-m",
            "bytewax.run",
            flow_path,
            # With 2 worker per process to ensure `cluster_main` is
            # used.
            "-w",
            "2",
            # Set snapshot interval to 0 so that the output is written
            # to the file as soon as possible
            "-s",
            "0",
        ]
        proc = subprocess.Popen(
            args,
            # Use PIPE to check the content of stdout
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _assert_can_be_ctrl_c(proc, tmp_file)


@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_testing_cluster_can_be_ctrl_c(tmp_path):
    """Test that we can stop cluster execution by sending SIGINT (ctrl+c)."""
    tmp_path = tmp_path / "out.txt"

    with open(tmp_path, "w+b") as tmp_file:
        # The dataflow we want to run is in ./test_flows/simple.py
        flow_path = f"pytests.test_flows.simple:get_flow('{tmp_file.name}')"
        args = [
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
        proc = subprocess.Popen(
            args,
            # Use PIPE to check the content of stdout
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        _assert_can_be_ctrl_c(proc, tmp_file)
