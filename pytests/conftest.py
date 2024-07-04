"""`pytest` config for `pytests/`.

This sets up our fixtures and logging.

"""

import os
import shutil

from datetime import datetime, timezone

from bytewax.recovery import RecoveryConfig, SnapshotMode
from bytewax.testing import cluster_main, run_main
from bytewax.tracing import setup_tracing
from bytewax.backup import file_system_backup
from pytest import fixture


@fixture(params=["run_main", "cluster_main-1thread", "cluster_main-2thread"])
def entry_point_name(request):
    """Run a version of the test for each execution point.

    You probably want to use the `entry_point` fixture to get a
    callable instead of the name here.

    There will be `"run_main"` for single in-thread, and
    `"cluster_main-2thread"` for launching 2 worker sub-threads.

    """
    return request.param


def _wrapped_cluster_main1x2(*args, **kwargs):
    return cluster_main(*args, [], 0, worker_count_per_proc=2, **kwargs)


def _wrapped_cluster_main1x1(*args, **kwargs):
    return cluster_main(*args, [], 0, **kwargs)


@fixture
def entry_point(entry_point_name):
    """Run a version of this test for each execution point.

    See `entry_point_name` for options.

    """
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "cluster_main-1thread":
        return _wrapped_cluster_main1x1
    elif entry_point_name == "cluster_main-2thread":
        return _wrapped_cluster_main1x2
    else:
        msg = "unknown entry point name: {request.param!r}"
        raise ValueError(msg)


@fixture
def recovery_config_batch(tmp_path):
    """Generate a recovery config with snapshot_mode set to Batch."""
    os.mkdir(tmp_path / "backup")
    yield RecoveryConfig(
        tmp_path,
        backup=file_system_backup(tmp_path / "backup"),
        snapshot_mode=SnapshotMode.Batch,
    )
    shutil.rmtree(tmp_path)


@fixture
def recovery_config_immediate(tmp_path):
    """Generate a recovery config with snapshot_mode set to Immediate."""
    os.mkdir(tmp_path / "backup")
    yield RecoveryConfig(
        tmp_path,
        backup=file_system_backup(tmp_path / "backup"),
        snapshot_mode=SnapshotMode.Immediate,
    )
    shutil.rmtree(tmp_path)


@fixture
def now():
    """Get the current `datetime` in UTC."""
    yield datetime.now(timezone.utc)


def pytest_addoption(parser):
    """Add a `--bytewax-log-level` CLI option to pytest.

    This will control the `setup_tracing` log level.

    """
    parser.addoption(
        "--bytewax-log-level",
        action="store",
        choices=["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
    )


def pytest_configure(config):
    """This will run on pytest init."""
    log_level = config.getoption("--bytewax-log-level")
    if log_level:
        setup_tracing(log_level=log_level)
