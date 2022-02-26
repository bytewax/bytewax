import os
import signal
import threading
from sys import exit

from bytewax import cluster_main, Dataflow, inp, run, run_cluster

from multiprocess import Manager, Process

from pytest import mark, raises


def test_run():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run(flow, inp.fully_ordered(range(3)))
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_cluster():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow, inp.fully_ordered(range(3)), proc_count=2, worker_count_per_proc=2
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_requires_capture():
    flow = Dataflow()

    with raises(ValueError):
        run(flow, enumerate(range(3)))


def test_run_cluster_requires_capture():
    flow = Dataflow()

    with raises(ValueError):
        run_cluster(flow, enumerate(range(3)))


def test_run_reraises_exception():
    def boom(item):
        raise RuntimeError()

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(RuntimeError):
        run(flow, enumerate(range(3)))


@mark.skip(
    reason="Timely is currently double panicking in cluster mode and that causes pool.join() to hang; it can be ctrl-c'd though"
)
def test_run_cluster_reraises_exception():
    def boom(item):
        if item == 0:
            raise RuntimeError()
        else:
            return item

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(RuntimeError):
        run_cluster(flow, enumerate(range(3)), proc_count=2)


def test_run_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def mapper(item):
            is_running.set()

        flow = Dataflow()
        flow.map(mapper)
        flow.capture()

        try:
            for epoch_item in run(flow, inp.fully_ordered(range(1000))):
                out.append(epoch_item)
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    if os.name == "nt":
        sig = signal.CTRL_C_EVENT
    else:
        sig = signal.SIGINT
    os.kill(test_proc.pid, sig)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000


def test_run_cluster_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def mapper(item):
            is_running.set()

        flow = Dataflow()
        flow.map(mapper)
        flow.capture()

        try:
            for epoch_item in run_cluster(
                flow,
                inp.fully_ordered(range(1000)),
                proc_count=2,
                worker_count_per_proc=2,
            ):
                out.append(epoch_item)
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    if os.name == "nt":
        sig = signal.CTRL_C_EVENT
    else:
        sig = signal.SIGINT
    os.kill(test_proc.pid, sig)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000


def test_cluster_main_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def input_builder(worker_index, worker_count):
            return inp.fully_ordered(range(1000))

        def output_builder(worker_index, worker_count):
            def out_handler(epoch_item):
                out.append(epoch_item)

            return out_handler

        def mapper(item):
            is_running.set()

        flow = Dataflow()
        flow.map(mapper)
        flow.capture()

        try:
            cluster_main(flow, input_builder, output_builder, [], 0, 1)
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    if os.name == "nt":
        sig = signal.CTRL_C_EVENT
    else:
        sig = signal.SIGINT
    os.kill(test_proc.pid, sig)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000
