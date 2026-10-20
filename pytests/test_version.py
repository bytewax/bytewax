from importlib.metadata import version

import bytewax


def test_version_is_exposed():
    """The installed version is accessible as `bytewax.__version__`."""
    assert bytewax.__version__ == version("bytewax")


def test_git_revision_is_exposed():
    """The build's git revision is accessible as `__git_revision__`."""
    assert isinstance(bytewax.__git_revision__, str)
    assert bytewax.__git_revision__


def test_git_dirty_is_exposed():
    """The build tree's dirty state is accessible as `__git_dirty__`."""
    assert isinstance(bytewax.__git_dirty__, bool)
