"""Implementation of a consistent hash for "exchanging" data. Hence
"exhash".

Bytewax uses this to ensure that items are routed to workers correctly
and consistently. We cannot use Python's `__hash__` because is not
consistent across processes by default.

Bytewax calls `exhash` internally to do routing.

If you need to route on a new key type, register a new version as is
done below in your own code. You _must_ make sure that if two objects
are `exhash(x) == exhash(y)` they also are `x == y`.

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
    # This will throw on gigantic arbitrary precision integers.
    h.update(key.to_bytes(8, byteorder="little"))
    return h


@exhash.register
def _(key: str, h=None):
    if h is None:
        h = new_hasher()
    h.update(key.encode("utf8"))
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
