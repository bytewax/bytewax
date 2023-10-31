from bytewax.testing import TestingSource, ffwd_iter
from pytest import raises


def test_ffwd_iter():
    it = iter(range(5))
    assert next(it) == 0
    ffwd_iter(it, 3)
    assert next(it) == 4
    with raises(StopIteration):
        next(it)


def test_input(now):
    inp = TestingSource(range(3))
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0]
    assert part.next_batch(now) == [1]
    assert part.next_batch(now) == [2]
    with raises(StopIteration):
        part.next_batch(now)
    part.close()


def test_input_resume_state(now):
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


def test_input_batch_size(now):
    inp = TestingSource(range(5), batch_size=2)
    part = inp.build_part(now, "iterable", None)
    assert part.next_batch(now) == [0, 1]
    assert part.next_batch(now) == [2, 3]
    assert part.next_batch(now) == [4]
    part.close()
