import os
import signal
from sys import exit

from pytest import fixture, mark, raises, skip

from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main, run_main, spawn_cluster
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig


@fixture(params=["run_main", "spawn_cluster", "cluster_main"])
def entry_point_name(request):
    return request.param


@fixture
def entry_point(entry_point_name):
    if entry_point_name == "run_main":
        return run_main
    elif entry_point_name == "spawn_cluster":
        return lambda flow: spawn_cluster(flow, proc_count=2, worker_count_per_proc=2)
    elif entry_point_name == "cluster_main":
        return lambda flow: cluster_main(flow, [], 0, worker_count_per_proc=2)
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


def test_run(entry_point, out):
    inp = range(3)
    flow = Dataflow(TestingInputConfig(inp))
    flow.map(lambda x: x + 1)
    flow.capture(TestingOutputConfig(out))

    entry_point(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_reraises_exception(entry_point, out, entry_point_name):
    if entry_point_name == "spawn_cluster":
        skip(
            "Timely is currently double panicking in cluster mode and that causes"
            " pool.join() to hang; it can be ctrl-c'd though"
        )

    inp = range(3)
    flow = Dataflow(TestingInputConfig(inp))

    def boom(item):
        if item == 0:
            raise RuntimeError("BOOM")
        else:
            return item

    flow.map(boom)
    flow.capture(TestingOutputConfig(out))

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
def test_can_be_ctrl_c(mp_ctx, entry_point):
    with mp_ctx.Manager() as man:
        is_running = man.Event()
        out = man.list()

        def proc_main():
            inp = range(1000)
            flow = Dataflow(TestingInputConfig(inp))

            def mapper(item):
                is_running.set()
                return item

            flow.map(mapper)
            flow.capture(TestingOutputConfig(out))

            try:
                entry_point(flow)
            except KeyboardInterrupt:
                exit(99)

        test_proc = mp_ctx.Process(target=proc_main)
        test_proc.start()

        assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
        os.kill(test_proc.pid, signal.SIGINT)
        test_proc.join(timeout=10.0)

        assert test_proc.exitcode == 99
        assert len(out) < 1000


def test_requires_capture(entry_point):
    inp = range(3)
    flow = Dataflow(TestingInputConfig(inp))

    with raises(ValueError):
        entry_point(flow)
