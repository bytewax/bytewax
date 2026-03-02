"""Tests for bytewax.__version__ (Fix #326)."""

import re
from importlib.metadata import version

import bytewax


def test_version_exists():
    """bytewax.__version__ attribute exists."""
    assert hasattr(bytewax, "__version__")


def test_version_is_string():
    """__version__ is a string."""
    assert isinstance(bytewax.__version__, str)


def test_version_format():
    """__version__ follows semver-like format."""
    assert re.match(r"^\d+\.\d+\.\d+", bytewax.__version__)


def test_version_matches_metadata():
    """__version__ matches importlib.metadata."""
    assert bytewax.__version__ == version("bytewax")
