import asyncio
import os
import signal
import threading
from sys import exit

from bytewax import inputs
from bytewax.execution import *

from multiprocess import Manager, Process

from pytest import mark, raises


def test_requires_capture():
    flow = Dataflow()

    def input_builder(worker_index, worker_count):
        return []

    def output_builder(worker_index, worker_count):
        def output_handler(epoch_item):
            pass

        return output_handler

    with raises(ValueError):
        WorkerBuilder.sync().build(
            flow, input_builder, output_builder, should_stop=lambda: False
        )


def test_run_main():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    def input_builder(worker_index, worker_count):
        assert worker_index == 0
        return [(0, 0), (1, 1), (2, 2)]

    out = []

    def output_builder(worker_index, worker_count):
        assert worker_index == 0
        return out.append

    asyncio.run(
        run_main(
            flow,
            input_builder,
            output_builder,
        )
    )

    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_supports_generator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run(flow, inputs.fully_ordered(range(3)))
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_supports_iterator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run(flow, enumerate(range(3)))
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_supports_list():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run(flow, [(0, 0), (1, 1), (2, 2)])
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_reraises_exception():
    def boom(item):
        return item / 0

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(ZeroDivisionError):
        list(run(flow, enumerate(range(3))))


@mark.skipif(
    os.name == "nt",
    reason="Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all processes on this console so interrupts pytest itself",
)
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
            for epoch_item in run(flow, inputs.fully_ordered(range(1000))):
                out.append(epoch_item)
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000


def test_cluster_main():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    def input_builder(worker_index, worker_count):
        return [(0, worker_index)]

    out = []

    def output_builder(worker_index, worker_count):
        return out.append

    cluster_main(
        flow,
        input_builder,
        output_builder,
        addresses=["localhost:2101"],
        proc_id=0,
        worker_count_per_proc=3,
    )

    assert sorted(out) == sorted([(0, 1), (0, 2), (0, 3)])


def test_cluster_main_reraises_exception():
    def boom(item):
        if item == 1:
            return item / 0
        else:
            return item

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    def input_builder(worker_index, worker_count):
        return [(0, worker_index)]

    out = []

    def output_builder(worker_index, worker_count):
        return out.append

    with raises(ZeroDivisionError):
        cluster_main(
            flow,
            input_builder,
            output_builder,
            addresses=["localhost:2101"],
            proc_id=0,
            worker_count_per_proc=2,
        )


@mark.skipif(
    os.name == "nt",
    reason="Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all processes on this console so interrupts pytest itself",
)
def test_cluster_main_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def input_builder(worker_index, worker_count):
            return inputs.fully_ordered(range(1000))

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
            cluster_main(
                flow,
                input_builder,
                output_builder,
                addresses=["localhost:2101"],
                proc_id=0,
                worker_count_per_proc=2,
            )
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000 * 2


def test_run_cluster_supports_generator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow, inputs.fully_ordered(range(3)), proc_count=2, worker_count_per_proc=2
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_cluster_supports_iterator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(flow, enumerate(range(3)), proc_count=2, worker_count_per_proc=2)
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_cluster_supports_list():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow, [(0, 0), (1, 1), (2, 2)], proc_count=2, worker_count_per_proc=2
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


def test_run_cluster_reraises_exception():
    def boom(item):
        if item == 1:
            return item / 0
        else:
            return item

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(ZeroDivisionError):
        out = run_cluster(
            flow, enumerate(range(3)), proc_count=2, worker_count_per_proc=2
        )
        list(out)


@mark.skipif(
    os.name == "nt",
    reason="Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all processes on this console so interrupts pytest itself",
)
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
                inputs.fully_ordered(range(1000)),
                proc_count=2,
                worker_count_per_proc=2,
            ):
                out.append(epoch_item)
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=1.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000
