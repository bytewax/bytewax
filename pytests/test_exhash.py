from pytest import raises

from bytewax.exhash import exhash


def test_exhash_int():
    assert exhash(5).digest() == exhash(5).digest()
    assert exhash(-5).digest() == exhash(-5).digest()
    assert exhash(5).digest() != exhash(4).digest()


def test_exhash_str():
    assert exhash("hello").digest() == exhash("hello").digest()
    assert exhash("hello").digest() != exhash("world").digest()


def test_exhash_bytes():
    assert exhash(b"hello").digest() == exhash(b"hello").digest()
    assert exhash(b"hello").digest() != exhash(b"world").digest()


def test_exhash_tuple():
    assert exhash((1, 2)).digest() == exhash((1, 2)).digest()
    assert exhash((1, 2)).digest() != exhash((2, 1)).digest()


def test_exhash_frozenset():
    assert (
        exhash(frozenset([1, 2, 3])).digest() == exhash(frozenset([3, 2, 1])).digest()
    )
    assert (
        exhash(frozenset([1, 2, 3])).digest() != exhash(frozenset([4, 5, 6])).digest()
    )


def test_cant_exhash_list():
    with raises(NotImplementedError):
        exhash([1, 2, 3]).digest()


def test_cant_exhash_set():
    with raises(NotImplementedError):
        exhash({1, 2, 3}).digest()


def test_cant_exhash_dict():
    with raises(NotImplementedError):
        exhash({"a": 1, "b": 2}).digest()
