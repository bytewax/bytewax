from bytewax.testing import TestingInput
from pytest import raises


def test_input():
    inp = TestingInput(range(3))
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0]
    assert part.next_batch() == [1]
    assert part.next_batch() == [2]
    with raises(StopIteration):
        part.next_batch()
    part.close()


def test_input_resume_state():
    inp = TestingInput(range(3))
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0]
    assert part.next_batch() == [1]
    resume_state = part.snapshot()
    assert resume_state == 2
    assert part.next_batch() == [2]
    part.close()

    inp = TestingInput(range(3))
    part = inp.build_part("iterable", resume_state)
    assert part.snapshot() == resume_state
    assert part.next_batch() == [2]
    with raises(StopIteration):
        part.next_batch()
    part.close()


def test_input_batch_size():
    inp = TestingInput(range(5), batch_size=2)
    part = inp.build_part("iterable", None)
    assert part.next_batch() == [0, 1]
    assert part.next_batch() == [2, 3]
    assert part.next_batch() == [4]
    part.close()
