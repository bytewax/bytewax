"""Utilities for error handling.

When exceptions are thrown in Python from user code, errors
are wrapped with a {py:obj}`bytewax.errors.BytewaxRuntimeError`. and
annotated with additional context from the Rust runtime.
"""


class BytewaxRuntimeError(RuntimeError):
    """A RuntimeError thrown by the Bytewax Runtime."""

    pass
