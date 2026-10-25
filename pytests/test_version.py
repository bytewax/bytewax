from importlib.metadata import version

import bytewax


def test_version_is_exposed():
    """The installed version is accessible as `bytewax.__version__`."""
    assert bytewax.__version__ == version("bytewax")
