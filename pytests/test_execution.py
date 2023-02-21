import os
import signal
from sys import exit

from pytest import mark, raises, skip

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import PartInput
from bytewax.outputs import ManualOutputConfig, TestingOutputConfig
from bytewax.testing import TestingInput


def test_run(entry_point, out):
    flow = Dataflow()
    inp = range(3)
    flow.input("inp", TestingInput(inp))
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
    flow.input("inp", TestingInput(inp))

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
            flow.input("inp", TestingInput(inp))

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
    flow.input("inp", TestingInput(inp))

    with raises(ValueError):
        entry_point(flow)


def test_parts_hashed_to_workers(mp_ctx):
    with mp_ctx.Manager() as man:
        proc_count = 3
        worker_count_per_proc = 2
        worker_count = proc_count * worker_count_per_proc

        flow = Dataflow()

        class LabeledPartInput(PartInput):
            def list_parts(self):
                # We have to have len(partitions) >= worker_count
                # otherwise we activate random load-balancing.
                return [str(i) * 3 for i in range(worker_count)]

            def build_part(self, for_part, resume_state):
                # The input for this partition is a singular "111",
                # "222"... No resume state.
                return [(None, for_part)]

        flow.input("inp", LabeledPartInput())

        # We then don't have any steps that exchange (except for the
        # random load-balancing, but we have enough partitions to
        # disable that), and immediately save the item into a dict by
        # worker, so we know what worker was given the partition.
        out = man.dict()
        for worker_index in range(worker_count):
            out[worker_index] = man.list()

        def output_builder(worker_index, worker_count):
            def output_handler(item):
                out[worker_index].append(item)

            return output_handler

        flow.capture(ManualOutputConfig(output_builder))

        spawn_cluster(
            flow, proc_count=proc_count, worker_count_per_proc=worker_count_per_proc
        )

        # We should see that the partition count is spread out among
        # all workers. **This depends on the routing hashing algo!**
        # So if we change that this fails. But it should be
        # deterministic.
        assert {str(k): sorted(v) for k, v in out.items()} == {
            "0": ["444"],
            "1": [],
            "2": ["000", "222"],
            "3": ["333"],
            "4": ["555"],
            "5": ["111"],
        }
