from pytest import raises

from bytewax.testing import TestingInput


def test_input_resume_state():
    inp = TestingInput(range(3))
    part = inp.build_part("iter", None)
    assert part.next() == 0
    assert part.next() == 1
    resume_state = part.snapshot()
    assert part.next() == 2
    part.close()

    inp = TestingInput(range(3))
    part = inp.build_part("iter", resume_state)
    assert part.snapshot() == resume_state
    assert part.next() == 2
    with raises(StopIteration):
        part.next()
    part.close()
