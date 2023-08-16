"""Custom pytest config for `pytests/`."""
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import cluster_main, run_main
from bytewax.tracing import setup_tracing
from pytest import fixture


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


@fixture(params=["run_main", "cluster_main-2thread"])
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


@fixture
def entry_point(entry_point_name):
    """Run a version of this test for each execution point.

    See `entry_point_name` for options.

    """
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "cluster_main-2thread":
        return _wrapped_cluster_main1x2
    else:
        msg = "unknown entry point name: {request.param!r}"
        raise ValueError(msg)


@fixture
def recovery_config(tmp_path):
    """Generate a recovery config.

    It will point to a single partition recovery store.

    """
    init_db_dir(tmp_path, 1)
    yield RecoveryConfig(str(tmp_path))
