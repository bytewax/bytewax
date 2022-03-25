import asyncio
import os
import signal
import threading
from sys import exit

from bytewax import inputs
from bytewax.execution import *

from multiprocessing import Manager, Process

from pytest import mark, raises


def test_requires_capture():
    flow = Dataflow()

    def input_builder(worker_index, worker_count):
        return []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch, item in epoch_items:
            pass

    with raises(ValueError):
        WorkerBuilder.sync().build(
            flow, input_builder, output_builder, should_stop=lambda: False
        )


def test_run_main():
    def mapper(worker_x):
        worker, x = worker_x
        return (worker, x + 1)

    flow = Dataflow()
    flow.map(mapper)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    out = []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

    asyncio.run(run_main(flow, input_builder, output_builder))

    assert sorted(out) == sorted([(0, (0, 1)), (0, (0, 2)), (0, (0, 3))])


def test_run_main_reraises_exception():
    def boom(worker_x):
        worker, x = worker_x
        if x == 1:
            return x / 0
        else:
            return x

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    out = []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

    with raises(ZeroDivisionError):
        asyncio.run(run_main(flow, input_builder, output_builder))


@mark.skipif(
    os.name == "nt",
    reason="Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all processes on this console so interrupts pytest itself",
)
def test_run_main_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def input_builder(worker_index, worker_count):
            return inputs.fully_ordered(range(1000))

        async def output_builder(worker_index, worker_count, epoch_items):
            async for epoch_item in epoch_items:
                out.append(epoch_item)

        def mapper(item):
            is_running.set()
            return item

        flow = Dataflow()
        flow.map(mapper)
        flow.capture()

        try:
            asyncio.run(run_main(flow, input_builder, output_builder))
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000 * 2


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
            return item

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

    assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000


def test_cluster_main():
    def mapper(worker_x):
        worker, x = worker_x
        return (worker, x + 1)

    flow = Dataflow()
    flow.map(mapper)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    out = []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

    cluster_main(
        flow,
        input_builder,
        output_builder,
        addresses=["localhost:2101"],
        proc_id=0,
        worker_count_per_proc=2,
    )

    assert sorted(out) == sorted(
        [
            (0, (0, 1)),
            (0, (0, 2)),
            (0, (0, 3)),
            (0, (1, 1)),
            (0, (1, 2)),
            (0, (1, 3)),
        ]
    )


def test_cluster_main_reraises_exception():
    def boom(worker_x):
        worker, x = worker_x
        if x == 1:
            return x / 0
        else:
            return x

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    out = []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

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

        async def output_builder(worker_index, worker_count, epoch_items):
            async for epoch_item in epoch_items:
                out.append(epoch_item)

        def mapper(item):
            is_running.set()
            return item

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

    assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000 * 2


def test_spawn_cluster():
    manager = Manager()
    out = manager.list()

    def mapper(worker_x):
        worker, x = worker_x
        return (worker, x + 1)

    flow = Dataflow()
    flow.map(mapper)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

    asyncio.run(
        spawn_cluster(
            flow,
            input_builder,
            output_builder,
            proc_count=2,
            worker_count_per_proc=2,
        )
    )

    assert sorted(out) == sorted(
        [
            (0, (0, 1)),
            (0, (0, 2)),
            (0, (0, 3)),
            (0, (1, 1)),
            (0, (1, 2)),
            (0, (1, 3)),
            (0, (2, 1)),
            (0, (2, 2)),
            (0, (2, 3)),
            (0, (3, 1)),
            (0, (3, 2)),
            (0, (3, 3)),
        ]
    )


def test_spawn_cluster_reraises_exception():
    def boom(worker_x):
        worker, x = worker_x
        if x == 1:
            return x / 0
        else:
            return x

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    def input_builder(worker_index, worker_count):
        for i in range(3):
            yield (0, (worker_index, i))

    out = []

    async def output_builder(worker_index, worker_count, epoch_items):
        async for epoch_item in epoch_items:
            out.append(epoch_item)

    with raises(RuntimeError):
        asyncio.run(
            spawn_cluster(
                flow,
                input_builder,
                output_builder,
                proc_count=2,
                worker_count_per_proc=2,
            )
        )


@mark.skipif(
    os.name == "nt",
    reason="Sending os.kill(test_proc.pid, signal.CTRL_C_EVENT) sends event to all processes on this console so interrupts pytest itself",
)
def test_spawn_cluster_can_be_ctrl_c():
    manager = Manager()
    is_running = manager.Event()
    out = manager.list()

    def proc_main():
        def input_builder(worker_index, worker_count):
            return inputs.fully_ordered(range(1000))

        async def output_builder(worker_index, worker_count, epoch_items):
            async for epoch_item in epoch_items:
                out.append(epoch_item)

        def mapper(item):
            is_running.set()
            return item

        flow = Dataflow()
        flow.map(mapper)
        flow.capture()

        try:
            asyncio.run(
                spawn_cluster(
                    flow,
                    input_builder,
                    output_builder,
                    proc_count=2,
                    worker_count_per_proc=2,
                )
            )
        except KeyboardInterrupt:
            exit(99)

    test_proc = Process(target=proc_main)
    test_proc.start()

    assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000 * 2


@mark.skipif(
    os.name == "nt" and os.environ.get("GITHUB_ACTION") is not None,
    reason="Hangs in Windows GitHub Actions",
)
def test_run_cluster_supports_generator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow, inputs.fully_ordered(range(3)), proc_count=2, worker_count_per_proc=2
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


@mark.skipif(
    os.name == "nt" and os.environ.get("GITHUB_ACTION") is not None,
    reason="Hangs in Windows GitHub Actions",
)
def test_run_cluster_supports_iterator():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(flow, enumerate(range(3)), proc_count=2, worker_count_per_proc=2)
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


@mark.skipif(
    os.name == "nt" and os.environ.get("GITHUB_ACTION") is not None,
    reason="Hangs in Windows GitHub Actions",
)
def test_run_cluster_supports_list():
    flow = Dataflow()
    flow.map(lambda x: x + 1)
    flow.capture()

    out = run_cluster(
        flow, [(0, 0), (1, 1), (2, 2)], proc_count=2, worker_count_per_proc=2
    )
    assert sorted(out) == sorted([(0, 1), (1, 2), (2, 3)])


@mark.skipif(
    os.name == "nt" and os.environ.get("GITHUB_ACTION") is not None,
    reason="Hangs in Windows GitHub Actions",
)
def test_run_cluster_reraises_exception():
    def boom(item):
        if item == 1:
            return item / 0
        else:
            return item

    flow = Dataflow()
    flow.map(boom)
    flow.capture()

    with raises(RuntimeError):
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
            return item

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

    assert is_running.wait(timeout=5.0), "Timeout waiting for test proc to start"
    os.kill(test_proc.pid, signal.SIGINT)
    test_proc.join()

    assert test_proc.exitcode == 99
    assert len(out) < 1000
