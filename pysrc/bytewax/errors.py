"""Utilities for error handling."""


class BytewaxRuntimeError(RuntimeError):
    """A RuntimeError thrown by the Bytewax Runtime.

    When exceptions are thrown in Python from user code, errors
    can be chained from this error to provide additional context
    from the Rust runtime.
    """

    pass
