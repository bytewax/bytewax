import os
import sys
from unittest.mock import patch

from bytewax.run import _parse_args, _prepare_import


def test_parse_args_environ(tmpdir):
    # We don't pass process_id, or "addresses",
    # but we set the env vars for them
    testargs = [
        "fake_command",
        "examples.basic:flow",
    ]

    hostpath = tmpdir / "hosts.txt"
    with open(hostpath, "w") as hostfile:
        hostfile.write("localhost:1234\n")
        hostfile.write("localhost:5678\n")
        hostfile.write("\n")

    testenv = os.environ.copy()
    testenv["BYTEWAX_HOSTFILE_PATH"] = str(hostpath)
    testenv["BYTEWAX_POD_NAME"] = "stateful_set-0"
    testenv["BYTEWAX_STATEFULSET_NAME"] = "stateful_set"
    # Mock sys.argv to test that the parsing phase works well
    with patch.object(sys, "argv", testargs):
        with patch.object(os, "environ", testenv):
            parsed = _parse_args()
            assert parsed.process_id == 0
            assert parsed.addresses == "localhost:1234;localhost:5678"


def test_prepare_import_file():
    mod_str, attr_str = _prepare_import("examples/basic.py:flow")
    assert mod_str == "examples.basic"
    assert attr_str == "flow"


def test_prepare_import_package():
    mod_str, attr_str = _prepare_import("examples.basic:flow")
    assert mod_str == "examples.basic"
    assert attr_str == "flow"
