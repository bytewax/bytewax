from bytewax.testing import TestingSource, ffwd_iter
from pytest import raises


def test_ffwd_iter():
    it = iter(range(5))
    assert next(it) == 0
    ffwd_iter(it, 3)
    assert next(it) == 4
    with raises(StopIteration):
        next(it)


def test_input():
    inp = TestingSource(range(3))
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0]
    assert part.next_batch() == [1]
    assert part.next_batch() == [2]
    with raises(StopIteration):
        part.next_batch()
    part.close()


def test_input_resume_state():
    inp = TestingSource(range(3))
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0]
    assert part.next_batch() == [1]
    resume_state = part.snapshot()
    assert resume_state == 2
    assert part.next_batch() == [2]
    part.close()

    inp = TestingSource(range(3))
    part = inp.build_part("iterable", resume_state)
    assert part.snapshot() == resume_state
    assert part.next_batch() == [2]
    with raises(StopIteration):
        part.next_batch()
    part.close()


def test_input_batch_size():
    inp = TestingSource(range(5), batch_size=2)
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0, 1]
    assert part.next_batch() == [2, 3]
    assert part.next_batch() == [4]
    part.close()


def test_input_eof():
    inp = TestingSource(
        [0, 1, TestingSource.EOF, 2, 3, 4, TestingSource.EOF, 5], batch_size=3
    )
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0, 1]
    with raises(StopIteration):
        part.next_batch()
    resume_state = part.snapshot()
    assert resume_state == 3
    part.close()

    part = inp.build_part("iterable", resume_state)
    assert part.next_batch() == [2, 3, 4]
    assert part.next_batch() == []
    with raises(StopIteration):
        part.next_batch()
    resume_state = part.snapshot()
    assert resume_state == 7

    part = inp.build_part("iterable", resume_state)
    assert part.next_batch() == [5]
    with raises(StopIteration):
        part.next_batch()
    part.close()
