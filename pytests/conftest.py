from multiprocess import get_context
from pytest import fixture

from bytewax.execution import cluster_main, run_main, spawn_cluster


@fixture
def mp_ctx():
    return get_context("spawn")


@fixture(params=["run_main", "spawn_cluster", "cluster_main"])
def entry_point_name(request):
    return request.param


@fixture
def entry_point(entry_point_name):
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "spawn_cluster":

        def wrapped(*args, **kwargs):
            return spawn_cluster(*args, proc_count=2, worker_count_per_proc=2, **kwargs)

        return wrapped
    elif entry_point_name == "cluster_main":

        def wrapped(*args, **kwargs):
            return cluster_main(*args, [], 0, worker_count_per_proc=2, **kwargs)

        return wrapped
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def out(entry_point_name, request):
    if entry_point_name == "run_main":
        yield []
    elif entry_point_name == "spawn_cluster":
        mp_ctx = request.getfixturevalue("mp_ctx")
        with mp_ctx.Manager() as man:
            yield man.list()
    elif entry_point_name == "cluster_main":
        yield []
    else:
        raise ValueError("unknown entry point name: {request.param!r}")


@fixture
def inp(entry_point_name, request):
    if entry_point_name == "run_main":
        yield []
    elif entry_point_name == "spawn_cluster":
        mp_ctx = request.getfixturevalue("mp_ctx")
        with mp_ctx.Manager() as man:
            yield man.list()
    elif entry_point_name == "cluster_main":
        yield []
    else:
        raise ValueError("unknown entry point name: {request.param!r}")
