from pytest import raises

from bytewax.testing import TestingInput


def test_input_resume_state():
    inp = TestingInput(range(3))
    part = inp.build_part("iter", None)
    assert part.snapshot() == -1
    assert part.next() == 0
    assert part.snapshot() == 0
    assert part.next() == 1
    assert part.snapshot() == 1
    assert part.next() == 2
    assert part.snapshot() == 2

    inp = TestingInput(range(3))
    part = inp.build_part("iter", 1)
    assert part.snapshot() == 1
    assert part.next() == 2
    assert part.snapshot() == 2
    with raises(StopIteration):
        part.next()
    assert part.snapshot() == 2
