"""Tests for bytewax.__version__ — issue #326."""

import re

import pytest

try:
    import bytewax._bytewax
    HAS_NATIVE = True
except ImportError:
    HAS_NATIVE = False

pytestmark = pytest.mark.skipif(
    not HAS_NATIVE,
    reason="Requires built native extension (maturin develop)",
)


def test_version_exists():
    """bytewax.__version__ attribute is accessible."""
    import bytewax

    assert hasattr(bytewax, "__version__")


def test_version_is_string():
    """__version__ is a string."""
    import bytewax

    assert isinstance(bytewax.__version__, str)


def test_version_format():
    """__version__ matches semver pattern."""
    import bytewax

    assert re.match(r"\d+\.\d+\.\d+", bytewax.__version__)
