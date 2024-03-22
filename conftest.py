"""Project-wide `pytest` config.

This sets up our documentation test config.

See the [documentation for
Sybil](https://sybil.readthedocs.io/en/latest/index.html) for details
here.

This config tells Sybil to read the _Python source files_ and look for
Markdown code blocks and attempt to parse them as docstrings. This
means it does not dynamically look at the docstrings of the installed
version of Bytewax, but just the source.

Docstrings from PyO3 will be checked via the `_bytewax.pyi` stubs
file.

See the "Writing Docs" article about all the ways to do that and
configure behavior from the code block itself.

"""
import doctest

from bytewax.tracing import setup_tracing
from sybil import Sybil
from sybil.parsers import myst


def pytest_addoption(parser):
    """Add a `--bytewax-log-level` CLI option to pytest.

    This will control the `setup_tracing` log level.

    """
    parser.addoption(
        "--bytewax-log-level",
        action="store",
        choices=["ERROR", "WARN", "INFO", "DEBUG", "TRACE"],
    )


def pytest_configure(config):
    """This will run on pytest init."""
    log_level = config.getoption("--bytewax-log-level")
    if log_level:
        setup_tracing(log_level=log_level)


doctest_option_flags = doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE

pytest_collect_file = Sybil(
    parsers=[
        myst.PythonCodeBlockParser(doctest_optionflags=doctest_option_flags),
        myst.SkipParser(),
    ],
    patterns=["*.md", "*.py", "*.pyi"],
    # There's a bug in Sybil that can't parse the much more
    # complicated Markdown that `autodoc2` generates. Ignore these
    # files. No big deal because they don't include tests anyway.
    excludes=[
        "docs/api/bytewax/*.md",
        "docs/guide/contributing/writing-docs.md",
    ],
).pytest()
