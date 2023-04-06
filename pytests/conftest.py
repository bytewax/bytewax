from multiprocess import get_context
from pytest import fixture

from bytewax.execution import cluster_main, run_main, spawn_cluster
from bytewax.recovery import SqliteRecoveryConfig
from bytewax.tracing import setup_tracing


def pytest_addoption(parser):
    parser.addoption(
        "--bytewax-log-level",
        action="store",
        choices=["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
    )


def pytest_configure(config):
    log_level = config.getoption("--bytewax-log-level")
    if log_level:
        setup_tracing(log_level=log_level)


@fixture
def mp_ctx():
    return get_context("spawn")


@fixture(params=["run_main", "spawn_cluster-2proc-2thread", "cluster_main-2thread"])
def entry_point_name(request):
    return request.param


def _wrapped_spawn_cluster2x2(*args, **kwargs):
    return spawn_cluster(*args, proc_count=2, worker_count_per_proc=2, **kwargs)


def _wrapped_cluster_main1x2(*args, **kwargs):
    return cluster_main(*args, [], 0, worker_count_per_proc=2, **kwargs)


@fixture
def entry_point(entry_point_name):
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "spawn_cluster-2proc-2thread":
        return _wrapped_spawn_cluster2x2
    elif entry_point_name == "cluster_main-2thread":
        return _wrapped_cluster_main1x2
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def out(entry_point_name, request):
    if entry_point_name.startswith("run_main"):
        yield []
    elif entry_point_name.startswith("spawn_cluster"):
        mp_ctx = request.getfixturevalue("mp_ctx")
        with mp_ctx.Manager() as man:
            yield man.list()
    elif entry_point_name.startswith("cluster_main"):
        yield []
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def inp(entry_point_name, request):
    if entry_point_name.startswith("run_main"):
        yield []
    elif entry_point_name.startswith("spawn_cluster"):
        mp_ctx = request.getfixturevalue("mp_ctx")
        with mp_ctx.Manager() as man:
            yield man.list()
    elif entry_point_name.startswith("cluster_main"):
        yield []
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def recovery_config(tmp_path):
    yield SqliteRecoveryConfig(str(tmp_path))
