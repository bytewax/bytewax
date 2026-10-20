"""Bytewax is a Python framework for building scalable dataflows."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("bytewax")
except PackageNotFoundError:  # pragma: no cover
    # The package metadata is unavailable, e.g. when running from a
    # source checkout that has not been built or installed yet.
    __version__ = "unknown"

__all__ = ["__version__"]
