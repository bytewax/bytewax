import os
import signal
from sys import exit

from pytest import mark, raises, skip

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput


def test_run(entry_point, out):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))
    flow.map(lambda x: x + 1)
    flow.output("out", TestingOutput(out))

    entry_point(flow)

    assert sorted(out) == sorted([1, 2, 3])


def test_reraises_exception(entry_point, out, entry_point_name):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))

    def boom(item):
        if item == 0:
            raise RuntimeError("BOOM")
        else:
            return item

    flow.map(boom)
    flow.output("out", TestingOutput(out))

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
def test_can_be_ctrl_c(entry_point):
    pass
    # with mp_ctx.Manager() as man:
    #     is_running = man.Event()
    #     out = man.list()
    #
    #     def proc_main():
    #         flow = Dataflow()
    #         inp = range(1000)
    #         flow.input("inp", TestingInput(inp))
    #
    #         def mapper(item):
    #             is_running.set()
    #             return item
    #
    #         flow.map(mapper)
    #         flow.output("out", TestingOutput(out))
    #
    #         try:
    #             entry_point(flow)
    #         except KeyboardInterrupt:
    #             exit(99)
    #
    #     test_proc = mp_ctx.Process(target=proc_main)
    #     test_proc.start()
    #
    #     assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    #     os.kill(test_proc.pid, signal.SIGINT)
    #     test_proc.join(timeout=10.0)
    #
    #     assert test_proc.exitcode == 99
    #     assert len(out) < 1000


def test_requires_input(entry_point):
    flow = Dataflow()
    out = []
    flow.output("out", TestingOutput(out))

    with raises(ValueError):
        entry_point(flow)


def test_requires_output(entry_point):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))

    with raises(ValueError):
        entry_point(flow)
