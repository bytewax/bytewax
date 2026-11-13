"""Bytewax is a Python framework for building scalable dataflows."""

from importlib.metadata import PackageNotFoundError, version

# Add-on packages install modules into `bytewax` (for example
# `bytewax.duckdb`) without an `__init__.py` of their own, as they did
# while `bytewax` was a namespace package. Keep finding them when they
# are in another `sys.path` entry than this file.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

try:
    __version__ = version("bytewax")
except PackageNotFoundError:
    # The package metadata is unavailable, e.g. when running from a
    # source checkout that has not been installed yet.
    __version__ = "unknown"

__all__ = ["__version__"]
