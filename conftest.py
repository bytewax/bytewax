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

from sybil import Sybil
from sybil.parsers import myst

doctest_option_flags = doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE

pytest_collect_file = Sybil(
    parsers=[
        myst.PythonCodeBlockParser(doctest_optionflags=doctest_option_flags),
        myst.SkipParser(),
    ],
    patterns=["*.md", "*.py", "*.pyi"],
).pytest()
