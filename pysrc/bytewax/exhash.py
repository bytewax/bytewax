"""`exhash` is a consistent hash that Bytewax calls internally to
route data to workers.

We do not use Python's `hash` because it is not consistent between
processes by default and do not want to force modifying hash behavior
in unrelated code via
[`PYTHONHASHSEED`](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONHASHSEED).

If you need to route on a new key type, register a new version as is
done below in your own code. You _must_ make sure that if two objects
are `x == y` they also are `exhash(x) == exhash(y)`.

"""
from functools import singledispatch
from hashlib import blake2b


@singledispatch
def exhash(key, h=None):
    """A consistent hash of a value."""
    raise NotImplementedError(f"{type(key)} isn't exhash-able")


def new_hasher():
    return blake2b(digest_size=8)


@exhash.register
def _(key: list, h=None):
    raise NotImplementedError("can't exhash mutable list")


@exhash.register
def _(key: set, h=None):
    raise NotImplementedError("can't exhash mutable set")


@exhash.register
def _(key: dict, h=None):
    raise NotImplementedError("can't exhash mutable dict")


@exhash.register
def _(key: int, h=None):
    if h is None:
        h = new_hasher()
    h.update(key.to_bytes(key.bit_length() // 8 + 1, byteorder="little", signed=True))
    return h


@exhash.register
def _(key: str, h=None):
    if h is None:
        h = new_hasher()
    h.update(key.encode())
    return h


@exhash.register
def _(key: bytes, h=None):
    if h is None:
        h = new_hasher()
    h.update(key)
    return h


@exhash.register
def _(key: tuple, h=None):
    if h is None:
        h = new_hasher()
    for x in key:
        h = exhash(x, h)
    return h


@exhash.register
def _(key: frozenset, h=None):
    if h is None:
        h = new_hasher()
    for x in sorted(key):
        h = exhash(x, h)
    return h
