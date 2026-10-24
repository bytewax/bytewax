"""Tests for `python -m bytewax.run` and its import / argument helpers."""

import os
import runpy
import subprocess
import sys
import uuid
import warnings
from datetime import timedelta
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, Iterator, List

import bytewax._bytewax
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import (
    _create_arg_parser,
    _locate_dataflow,
    _parse_args,
    _parse_timedelta,
    _prepare_import,
    _split_import_str,
)
from pytest import MonkeyPatch, fixture, mark, raises

FLOW_MODULE = """\
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource


def build(prefix="item", count=3):
    flow = Dataflow(f"{prefix}_flow")
    s = op.input("inp", flow, TestingSource(range(count)))
    s = op.map("fmt", s, lambda x: f"{prefix}-{x}")
    op.output("out", s, StdOutSink())
    return flow


flow = build()
not_a_flow = 42


def returns_not_a_flow():
    return "nope"


def raises_type_error():
    msg = "boom from inside the factory"
    raise TypeError(msg)
"""


@fixture
def flow_module(tmp_path: Path, monkeypatch: MonkeyPatch) -> Iterator[str]:
    """Write a uniquely named module of test dataflows; return its name."""
    name = f"flows_{uuid.uuid4().hex}"
    (tmp_path / f"{name}.py").write_text(FLOW_MODULE)
    monkeypatch.syspath_prepend(str(tmp_path))
    yield name
    sys.modules.pop(name, None)


def test_locate_dataflow_variable(flow_module):
    flow = _locate_dataflow(flow_module, "flow")

    assert isinstance(flow, Dataflow)
    assert flow.flow_id == "item_flow"


def test_locate_dataflow_factory_with_literal_args(flow_module):
    flow = _locate_dataflow(flow_module, " build('custom', count=5) ")

    assert isinstance(flow, Dataflow)
    assert flow.flow_id == "custom_flow"


def test_locate_dataflow_missing_module_reraises():
    with raises(ModuleNotFoundError, match="no_such_module_xyz"):
        _locate_dataflow("no_such_module_xyz", "flow")


def test_locate_dataflow_reraises_import_error_inside_module(tmp_path, monkeypatch):
    name = f"broken_{uuid.uuid4().hex}"
    (tmp_path / f"{name}.py").write_text("import no_such_dependency_xyz\n")
    monkeypatch.syspath_prepend(str(tmp_path))

    with raises(ModuleNotFoundError, match="no_such_dependency_xyz"):
        _locate_dataflow(name, "flow")


def test_locate_dataflow_rejects_unparsable_name(flow_module):
    with raises(SyntaxError, match="Failed to parse 'build\\('"):
        _locate_dataflow(flow_module, "build(")


def test_locate_dataflow_rejects_dotted_function_call(flow_module):
    with raises(TypeError, match="Function reference must be a simple name"):
        _locate_dataflow(flow_module, "mod.build()")


def test_locate_dataflow_rejects_non_literal_args(flow_module):
    with raises(ValueError, match="Failed to parse arguments as literal values"):
        _locate_dataflow(flow_module, "build(some_variable)")


def test_locate_dataflow_rejects_other_expressions(flow_module):
    with raises(ValueError, match="as an attribute name or function call"):
        _locate_dataflow(flow_module, "mod.flow")


def test_locate_dataflow_missing_attribute(flow_module):
    with raises(AttributeError, match="Failed to find attribute 'missing'"):
        _locate_dataflow(flow_module, "missing")


def test_locate_dataflow_factory_wrong_args(flow_module):
    with raises(TypeError, match="could not be called with the specified arguments"):
        _locate_dataflow(flow_module, "build(1, 2, 3)")


def test_locate_dataflow_factory_type_error_inside_is_not_wrapped(flow_module):
    with raises(TypeError, match="boom from inside the factory"):
        _locate_dataflow(flow_module, "raises_type_error()")


def test_locate_dataflow_variable_not_a_dataflow(flow_module):
    with raises(RuntimeError, match="A valid Bytewax dataflow was not obtained"):
        _locate_dataflow(flow_module, "not_a_flow")


def test_locate_dataflow_factory_not_returning_dataflow(flow_module):
    with raises(RuntimeError, match="A valid Bytewax dataflow was not obtained"):
        _locate_dataflow(flow_module, "returns_not_a_flow()")


@fixture
def clean_syspath(monkeypatch, tmp_path):
    """Restore `sys.path` afterwards and run from `tmp_path`.

    Import strings in these tests are relative paths, so they read the
    same on every OS.

    """
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.chdir(tmp_path)


def test_prepare_import_defaults_to_flow(tmp_path, clean_syspath):
    (tmp_path / "solo.py").write_text("")

    mod_str, attr_str = _prepare_import("solo.py")

    assert (mod_str, attr_str) == ("solo", "flow")
    assert sys.path[0] == os.path.realpath(tmp_path)


def test_prepare_import_package_module(tmp_path, clean_syspath):
    pkg = tmp_path / "pkg" / "sub"
    pkg.mkdir(parents=True)
    (tmp_path / "pkg" / "__init__.py").write_text("")
    (pkg / "__init__.py").write_text("")
    (pkg / "mod.py").write_text("")

    mod_str, attr_str = _prepare_import("pkg/sub/mod.py:get_flow('x')")

    assert (mod_str, attr_str) == ("pkg.sub.mod", "get_flow('x')")
    # The directory above the outermost package goes on the path.
    assert sys.path[0] == os.path.realpath(tmp_path)


def test_prepare_import_package_init(tmp_path, clean_syspath):
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")

    mod_str, _ = _prepare_import("pkg/__init__.py:flow")

    assert mod_str == "pkg"


def test_prepare_import_does_not_duplicate_path(tmp_path, clean_syspath):
    (tmp_path / "solo.py").write_text("")
    sys.path.insert(0, os.path.realpath(tmp_path))
    before = list(sys.path)

    _prepare_import("solo.py:flow")

    assert sys.path == before


@mark.parametrize(
    ("import_str", "expected"),
    [
        ("flow.py", ("flow.py", "flow")),
        ("pkg.mod:get_flow('a:b')", ("pkg.mod", "get_flow('a:b')")),
        ("/srv/app/flow.py:flow", ("/srv/app/flow.py", "flow")),
        ("C:\\proj\\flow.py:flow", ("C:\\proj\\flow.py", "flow")),
        ("c:/proj/flow.py", ("c:/proj/flow.py", "flow")),
        ("D:\\proj\\flow.py:get_flow('x')", ("D:\\proj\\flow.py", "get_flow('x')")),
    ],
)
def test_split_import_str(import_str, expected):
    # A Windows drive letter's colon is part of the path, on any OS.
    assert _split_import_str(import_str) == expected


def test_prepare_import_absolute_path(tmp_path, clean_syspath):
    (tmp_path / "solo.py").write_text("")

    mod_str, attr_str = _prepare_import(f"{tmp_path / 'solo.py'}:flow")

    assert (mod_str, attr_str) == ("solo", "flow")


def test_parse_timedelta():
    assert _parse_timedelta("90") == timedelta(seconds=90)


def test_arg_parser_help_mentions_env_vars():
    # argparse wraps long help lines; compare on normalized whitespace.
    help_text = " ".join(_create_arg_parser().format_help().split())

    assert "[env: BYTEWAX_RECOVERY_DIRECTORY]" in help_text
    assert "[env: BYTEWAX_SNAPSHOT_INTERVAL]" in help_text
    assert "[env: BYTEWAX_RECOVERY_BACKUP_INTERVAL]" in help_text


@fixture
def no_bytewax_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("BYTEWAX_"):
            monkeypatch.delenv(key)


def _parse(monkeypatch, *argv: str):
    monkeypatch.setattr(sys, "argv", ["python -m bytewax.run", *argv])
    return _parse_args()


def test_parse_args_defaults(monkeypatch, no_bytewax_env):
    args = _parse(monkeypatch, "mod:flow")

    assert args.import_str == "mod:flow"
    assert args.workers_per_process is None
    assert args.process_id is None
    assert args.addresses is None
    assert args.recovery_directory is None
    assert args.snapshot_interval is None
    assert args.backup_interval is None


def test_parse_args_from_env(monkeypatch, no_bytewax_env, tmp_path):
    monkeypatch.setenv("BYTEWAX_WORKERS_PER_PROCESS", "3")
    monkeypatch.setenv("BYTEWAX_PROCESS_ID", "1")
    monkeypatch.setenv("BYTEWAX_ADDRESSES", "a:1;b:2")
    monkeypatch.setenv("BYTEWAX_RECOVERY_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("BYTEWAX_SNAPSHOT_INTERVAL", "5")
    monkeypatch.setenv("BYTEWAX_RECOVERY_BACKUP_INTERVAL", "7")

    args = _parse(monkeypatch, "mod:flow")

    # argparse converts string defaults with the argument's `type`,
    # same as values given on the command line.
    assert args.workers_per_process == 3
    assert args.process_id == 1
    assert args.addresses == "a:1;b:2"
    assert args.recovery_directory == tmp_path
    assert args.snapshot_interval == timedelta(seconds=5)
    assert args.backup_interval == timedelta(seconds=7)


def test_parse_args_cli_overrides_env(monkeypatch, no_bytewax_env):
    monkeypatch.setenv("BYTEWAX_WORKERS_PER_PROCESS", "3")

    args = _parse(monkeypatch, "mod:flow", "-w", "5")

    assert args.workers_per_process == 5


def test_parse_args_process_id_requires_addresses(monkeypatch, no_bytewax_env, capsys):
    with raises(SystemExit):
        _parse(monkeypatch, "mod:flow", "-i", "0")

    assert "the addresses option is required" in capsys.readouterr().err


def test_parse_args_recovery_requires_intervals(monkeypatch, no_bytewax_env, capsys):
    with raises(SystemExit):
        _parse(monkeypatch, "mod:flow", "-r", "/tmp/recovery", "-s", "10")

    assert "must be set" in capsys.readouterr().err


def _run_main_module(monkeypatch, *argv: str) -> Dict[str, Any]:
    """Execute `bytewax.run` as `__main__` and capture `cli_main` kwargs."""
    calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(bytewax._bytewax, "cli_main", lambda **kw: calls.append(kw))
    monkeypatch.setattr(sys, "argv", ["python -m bytewax.run", *argv])
    with warnings.catch_warnings():
        # runpy warns because `bytewax.run` is already imported.
        warnings.simplefilter("ignore", RuntimeWarning)
        runpy.run_module("bytewax.run", run_name="__main__")
    (kwargs,) = calls
    return kwargs


def test_main_without_recovery(monkeypatch, no_bytewax_env, flow_module, tmp_path):
    monkeypatch.chdir(tmp_path)
    kwargs = _run_main_module(monkeypatch, f"{flow_module}.py:flow", "-w", "2")

    assert kwargs["flow"].flow_id == "item_flow"
    assert kwargs["workers_per_process"] == 2
    assert kwargs["process_id"] is None
    assert kwargs["recovery_config"] is None
    # Default epoch interval when there's no recovery.
    assert kwargs["epoch_interval"] == timedelta(seconds=10)
    assert "addresses" not in kwargs


def test_main_snapshot_interval_without_recovery(
    monkeypatch, no_bytewax_env, flow_module, tmp_path
):
    monkeypatch.chdir(tmp_path)
    kwargs = _run_main_module(monkeypatch, f"{flow_module}.py", "-s", "3")

    assert kwargs["epoch_interval"] == timedelta(seconds=3)


def test_main_with_recovery_and_addresses(
    monkeypatch, no_bytewax_env, flow_module, tmp_path
):
    monkeypatch.chdir(tmp_path)
    recovery_dir = tmp_path / "recovery"
    kwargs = _run_main_module(
        monkeypatch,
        f"{flow_module}.py:build('rec')",
        "-r",
        str(recovery_dir),
        "-s",
        "1",
        "-b",
        "2",
        "-i",
        "1",
        "-a",
        "host-a:2101;host-b:2101",
    )

    assert kwargs["flow"].flow_id == "rec_flow"
    assert kwargs["epoch_interval"] == timedelta(seconds=1)
    config = kwargs["recovery_config"]
    assert isinstance(config, RecoveryConfig)
    assert config.db_dir == str(recovery_dir)
    assert config.backup_interval == timedelta(seconds=2)
    assert kwargs["process_id"] == 1
    assert kwargs["addresses"] == ["host-a:2101", "host-b:2101"]


def test_cli_runs_dataflow(tmp_path):
    (tmp_path / "cli_flow.py").write_text(
        dedent(
            """\
            import bytewax.operators as op
            from bytewax.connectors.stdio import StdOutSink
            from bytewax.dataflow import Dataflow
            from bytewax.testing import TestingSource

            def get_flow(prefix):
                flow = Dataflow("cli")
                s = op.input("inp", flow, TestingSource(range(3)))
                s = op.map("fmt", s, lambda x: f"{prefix}-{x}")
                op.output("out", s, StdOutSink())
                return flow
            """
        )
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("BYTEWAX_")}

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "bytewax.run",
            "cli_flow.py:get_flow('row')",
            "-w",
            "2",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=tmp_path,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    assert sorted(proc.stdout.split()) == ["row-0", "row-1", "row-2"]
