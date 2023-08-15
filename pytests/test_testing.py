from bytewax.testing import TestingInput
from pytest import raises


def test_input_resume_state():
    inp = TestingInput(range(3))
    part = inp.build_part("iter", None)
    assert part.next_batch() == [0]
    assert part.next_batch() == [1]
    resume_state = part.snapshot()
    assert part.next_batch() == [2]
    part.close()

    inp = TestingInput(range(3))
    part = inp.build_part("iter", resume_state)
    assert part.snapshot() == resume_state
    assert part.next_batch() == [2]
    with raises(StopIteration):
        part.next_batch()
    part.close()
