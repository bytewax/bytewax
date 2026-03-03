"""Tests for clean interpreter finalization (no SIGSEGV on exit).

Verifies that SafePy<T> prevents the Python 3.13+ finalization
segfault where Py<T>::Drop dereferences freed type objects.
"""

import subprocess
import sys

from pytest import mark

SEGFAULT_MSG = (
    "SIGSEGV detected (exit code {rc}) during interpreter finalization. "
    "SafePy<T> guard may have regressed.\nstderr: {stderr}"
)


@mark.skipif(
    sys.version_info < (3, 13),
    reason="Finalization segfault only affects Python 3.13+",
)
def test_clean_finalization_simple_dataflow():
    """A simple dataflow subprocess exits cleanly without SIGSEGV."""
    script = (
        "import bytewax.operators as op\n"
        "from bytewax.dataflow import Dataflow\n"
        "from bytewax.testing import TestingSource, TestingSink, run_main\n"
        "\n"
        "out = []\n"
        "flow = Dataflow('test_df')\n"
        "s = op.input('inp', flow, TestingSource(range(100)))\n"
        "s = op.map('inc', s, lambda x: x + 1)\n"
        "op.output('out', s, TestingSink(out))\n"
        "run_main(flow)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        check=False,
    )
    if result.returncode in (-11, 139):
        raise AssertionError(
            SEGFAULT_MSG.format(
                rc=result.returncode,
                stderr=result.stderr.decode(errors="replace"),
            )
        )
    assert result.returncode == 0, (
        f"Subprocess exited with code {result.returncode}.\n"
        f"stdout: {result.stdout.decode(errors='replace')}\n"
        f"stderr: {result.stderr.decode(errors='replace')}"
    )


@mark.skipif(
    sys.version_info < (3, 13),
    reason="Finalization segfault only affects Python 3.13+",
)
def test_clean_finalization_stateful_dataflow():
    """A stateful dataflow subprocess exits cleanly without SIGSEGV.

    Exercises StatefulPartition Drop impls with Py_IsFinalizing guards.
    """
    script = (
        "import bytewax.operators as op\n"
        "from bytewax.dataflow import Dataflow\n"
        "from bytewax.testing import TestingSource, TestingSink, run_main\n"
        "\n"
        "def check(running_count, _item):\n"
        "    if running_count is None:\n"
        "        running_count = 0\n"
        "    running_count += 1\n"
        "    return (running_count, running_count)\n"
        "\n"
        "out = []\n"
        "flow = Dataflow('test_df')\n"
        "s = op.input('inp', flow, TestingSource(['a', 'a', 'b', 'b', 'a']))\n"
        "s = op.key_on('key', s, lambda x: x)\n"
        "s = op.stateful_map('count', s, check)\n"
        "op.output('out', s, TestingSink(out))\n"
        "run_main(flow)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        check=False,
    )
    if result.returncode in (-11, 139):
        raise AssertionError(
            SEGFAULT_MSG.format(
                rc=result.returncode,
                stderr=result.stderr.decode(errors="replace"),
            )
        )
    assert result.returncode == 0, (
        f"Subprocess exited with code {result.returncode}.\n"
        f"stdout: {result.stdout.decode(errors='replace')}\n"
        f"stderr: {result.stderr.decode(errors='replace')}"
    )
