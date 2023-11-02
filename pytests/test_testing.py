from datetime import timedelta

from bytewax.dataflow import Dataflow
from bytewax.inputs import AbortExecution
from bytewax.testing import TestingSink, TestingSource, ffwd_iter, run_main
from pytest import raises

ZERO_TD = timedelta(seconds=0)


def test_ffwd_iter():
    it = iter(range(5))
    assert next(it) == 0
    ffwd_iter(it, 3)
    assert next(it) == 4
    with raises(StopIteration):
        next(it)


def test_testing_source(now):
    inp = TestingSource(range(3))
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0]
    assert part.next_batch(now) == [1]
    assert part.next_batch(now) == [2]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()


def test_testing_source_resume_state(now):
    inp = TestingSource(range(3))
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0]
    assert part.next_batch(now) == [1]
    resume_state = part.snapshot()
    assert resume_state == 2
    assert part.next_batch(now) == [2]
    part.close()

    inp = TestingSource(range(3))
    part = inp.build_part(now, "iterable", resume_state)
    assert part.snapshot() == resume_state
    assert part.next_batch(now) == [2]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()


def test_testing_source_batch_size(now):
    inp = TestingSource(range(5), batch_size=2)
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0, 1]
    assert part.next_batch(now) == [2, 3]
    assert part.next_batch(now) == [4]
    part.close()


def test_testing_source_eof(now):
    inp = TestingSource([0, 1, 2, TestingSource.EOF(), 3, 4], batch_size=2)
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0, 1]
    assert part.next_batch(now) == [2]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()

    resume_state = part.snapshot()
    part = inp.build_part(now, "iterable", resume_state)
    assert part.next_batch(now) == [3, 4]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()


def test_testing_source_eof_run(recovery_config):
    inp = [0, 1, 2, TestingSource.EOF(), 3, 4]
    out = []

    flow = Dataflow()
    flow.input("inp", TestingSource(inp, batch_size=2))
    flow.output("out", TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]


def test_testing_source_abort(now):
    inp = TestingSource([0, 1, 2, TestingSource.ABORT(), 3, 4], batch_size=2)
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0, 1]
    resume_state = part.snapshot()
    assert part.next_batch(now) == [2]
    with raises(AbortExecution):
        part.next_batch(now)

    part = inp.build_part(now, "iterable", resume_state)
    assert part.next_batch(now) == [2, 3]
    assert part.next_batch(now) == [4]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()


def test_testing_source_abort_run(recovery_config):
    inp = [0, 1, 2, TestingSource.ABORT(), 3, 4]
    out = []

    flow = Dataflow()
    flow.input("inp", TestingSource(inp, batch_size=2))
    flow.output("out", TestingSink(out))

    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [0, 1, 2]

    out.clear()
    run_main(flow, epoch_interval=ZERO_TD, recovery_config=recovery_config)
    assert out == [3, 4]
