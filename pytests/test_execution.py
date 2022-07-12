import os
import signal
from sys import exit

from pytest import mark, raises

from bytewax import (
    cluster_main,
    Dataflow,
    run,
    run_cluster,
)
from bytewax.inputs import ManualInputConfig


def test_run():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run(flow, list(range(3)))
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_cluster(mp_ctx):
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow,
        range(5),
        proc_count=1,
        worker_count_per_proc=1,
        mp_ctx=mp_ctx,
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])

def test_run_cluster_multiple_workers(mp_ctx):
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow,
        range(5),
        proc_count=1,
        worker_count_per_proc=2,
        mp_ctx=mp_ctx,
    )
    # We can't *really* guarantee this kind of epoch assignment consistency with multiple
    # workers, but with a small range this should pass. Perhaps a better test would be that
    # all the input has been modified, regardless of epoch?
    assert sorted(out) == sorted([(0, 1), (0, 2), (1, 3), (1, 4), (2, 5)])

def test_run_requires_capture():
    flow = Dataflow()

    with raises(ValueError):
        run(flow, range(3))


def test_run_cluster_requires_capture(mp_ctx):
    flow = Dataflow()

    with raises(ValueError):
        run_cluster(flow, range(3), mp_ctx=mp_ctx)


def test_run_reraises_exception():
    def boom(item):
        raise RuntimeError()

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(RuntimeError):
        run(flow, range(3))


@mark.skip(
    reason=(
        "Timely is currently double panicking in cluster mode and that causes"
        " pool.join() to hang; it can be ctrl-c'd though"
    )
)
def test_run_cluster_reraises_exception(mp_ctx):
    def boom(item):
        if item == 0:
            raise RuntimeError()
        else:
            return item

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(RuntimeError):
        run_cluster(flow, range(3), proc_count=2, mp_ctx=mp_ctx)


@mark.skip(reason="Flakey in CI for some unknown reason")
@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_run_can_be_ctrl_c(mp_ctx):
    with mp_ctx.Manager() as man:
        is_running = man.Event()
        out = man.list()

        def proc_main():
            def mapper(item):
                is_running.set()
                return item

            flow = Dataflow()
            flow.map(mapper)
            flow.capture()

            try:
                for epoch_item in run(flow, range(1000)):
                    out.append(epoch_item)
            except KeyboardInterrupt:
                exit(99)

        test_proc = mp_ctx.Process(target=proc_main)
        test_proc.start()

        assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
        os.kill(test_proc.pid, signal.SIGINT)
        test_proc.join(timeout=10.0)

        assert test_proc.exitcode == 99
        assert len(out) < 1000


@mark.skip(reason="Flakey in CI for some unknown reason")
@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_run_cluster_can_be_ctrl_c(mp_ctx):
    with mp_ctx.Manager() as man:
        is_running = man.Event()
        out = man.list()

        def proc_main():
            def mapper(item):
                is_running.set()
                return item

            flow = Dataflow()
            flow.map(mapper)
            flow.capture()

            try:
                for epoch_item in run_cluster(
                    flow,
                    range(1000),
                    proc_count=2,
                    worker_count_per_proc=2,
                ):
                    out.append(epoch_item)
            except KeyboardInterrupt:
                exit(99)

        test_proc = mp_ctx.Process(target=proc_main)
        test_proc.start()

        assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
        os.kill(test_proc.pid, signal.SIGINT)
        test_proc.join(timeout=10.0)

        assert test_proc.exitcode == 99
        assert len(out) < 1000


@mark.skip(reason="Flakey in CI for some unknown reason")
@mark.skipif(
    os.name == "nt",
    reason=(
        "Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all"
        " processes on this console so interrupts pytest itself"
    ),
)
def test_cluster_main_can_be_ctrl_c(mp_ctx):
    with mp_ctx.Manager() as man:
        is_running = man.Event()
        out = man.list()

        def proc_main():
            def input_builder(worker_index, worker_count, resume_epoch):
                for item in list(range(1000)):
                    yield item

            def output_builder(worker_index, worker_count):
                def out_handler(epoch_item):
                    out.append(epoch_item)

                return out_handler

            def mapper(item):
                is_running.set()
                return item

            flow = Dataflow()
            flow.map(mapper)
            flow.capture()

            try:
                cluster_main(
                    flow,
                    ManualInputConfig(input_builder),
                    output_builder,
                    addresses=[],
                    proc_id=0,
                    worker_count_per_proc=2,
                )
            except KeyboardInterrupt:
                exit(99)

        test_proc = mp_ctx.Process(target=proc_main)
        test_proc.start()

        assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
        os.kill(test_proc.pid, signal.SIGINT)
        test_proc.join(timeout=10.0)

        assert test_proc.exitcode == 99
        assert len(out) < 1000
