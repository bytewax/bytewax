import os
import signal
from sys import exit

from pytest import mark, raises, skip

from bytewax.dataflow import Dataflow
from bytewax.inputs import TestingInputConfig
from bytewax.outputs import TestingOutputConfig


def test_run(entry_point, out):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInputConfig(inp))
    flow.map(lambda x: x + 1)
    flow.capture(TestingOutputConfig(out))

    entry_point(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_reraises_exception(entry_point, out, entry_point_name):
    if entry_point_name.startswith("spawn_cluster"):
        skip(
            "Timely is currently double panicking in cluster mode and that causes"
            " pool.join() to hang; it can be ctrl-c'd though"
        )

    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInputConfig(inp))

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
            flow = Dataflow()
            inp = range(1000)
            flow.input("inp", TestingInputConfig(inp))

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
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInputConfig(inp))

    with raises(ValueError):
        entry_point(flow)
