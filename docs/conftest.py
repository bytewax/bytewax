"""Hooks and code to tell pytest how to run our Markdown documentation
as a kind of doctest.

Each file is a separate "script". Each Python code block within each
file will be run in sequence. If there's a code block tagged
`{testoutput}`, it will be compared to the stdout of the previous
Python code block. Errors will be reported.

"""
from contextlib import redirect_stdout
from dataclasses import dataclass
from doctest import Example, OutputChecker
from io import StringIO
from pathlib import Path
from traceback import print_exc
from typing import Iterator

import myst_parser.main
from _pytest._code.code import ReprFileLocation, TerminalRepr
from _pytest._io import TerminalWriter
from pytest import File, Item


@dataclass
class Block:
    """Represents a code block in a Markdown file so we can reference it
    later.

    """

    path: Path
    content: str
    content_first_line: int
    content_last_line: int


class MdTestFailure(Exception):
    """Exception thrown when the expected output doesn't match the found
    output.

    """

    def __init__(self, found: str):
        self.found = found


class MdTestFailureRepr(TerminalRepr):
    """pytest class for pretty printing test results. Returned by
    `MdTestItem.repr_failure` to print nice things.

    """

    def __init__(
        self, checker: OutputChecker, source: Block, expected: Block, found: str
    ):
        super().__init__()
        self.checker = checker
        self.source = source
        self.expected = expected
        self.found = found

    # Formatting ideas taken from
    # https://github.com/pytest-dev/pytest/blob/63126643b9b02bce4bbccc94ea00bebdb8137975/src/_pytest/doctest.py
    def toterminal(self, tw: TerminalWriter) -> None:
        """Copy what pytest's built in doctest code does to print output
        mismatches.

        """
        # Pick the biggest lenght line numbers so the code is always
        # aligned on a 99, 100 rollover.
        digits = len(str(abs(self.source.content_last_line)))
        lines = self.source.content.splitlines()
        # Print the source that produced bad output.
        for offset, line in enumerate(lines):
            lineno = self.source.content_first_line + offset
            tw.line(f"{lineno:0>{digits}}    {line}")

        # Use the builtin doctest diff printer.
        example = Example(self.source.content, self.expected.content)
        tw.write(self.checker.output_difference(example, self.found, 0))

        # Write out the location of the expected output.
        expected_loc = ReprFileLocation(
            self.expected.path, self.expected.content_first_line, MdTestFailure.__name__
        )
        expected_loc.toterminal(tw)


class MdTestItem(Item):
    """Represents a single test for pytest to run.

    There's one of these for every code block in the Markdown input
    (paired with an optional output block).

    """

    def __init__(self, *, globs, source: Block, expected: Block, **kwargs):
        name = f"{expected.content_first_line}"
        super().__init__(name=name, **kwargs)

        self.globs = globs

        self.source = source
        self.expected = expected

        self.checker = OutputChecker()

    def runtest(self):
        """Called by pytest to actually run the test!

        `compile()` the source string, and `exec()` it, capturing all
        output for later comparison.

        """
        # We only want to run the source content of this code block,
        # so we have to have it in isolation. But we also want
        # exceptions thrown here to show the line numbers in the
        # original Markdown file to help with debugging. Hack to do
        # that. Idea taken from
        # https://github.com/simplistix/sybil/blob/11494c65deb0dfd34e225d3f0b38a6824406d94c/sybil/parsers/codeblock.py#L44
        lineno_adjusted_source = (
            "\n" * (self.source.content_first_line - 1) + self.source.content
        )
        code = compile(lineno_adjusted_source, self.source.path, "exec")

        capture = StringIO()
        with redirect_stdout(capture):
            # For some reason, if we're running something
            # representing a module, there's only a
            # globals dictionary. See
            # https://docs.python.org/3/library/functions.html#exec
            try:
                exec(code, self.globs, self.globs)
            except:
                print_exc(file=capture)

        found = capture.getvalue()
        if not self.checker.check_output(self.expected.content, found, 0):
            raise MdTestFailure(found)

    def reportinfo(self):
        """Called by pytest before pretty printing out failures.

        Use this to show which code block failed.

        """
        return (
            self.expected.path,
            self.expected.content_first_line,
            f"[mdtest] {self.expected.path.name}:{self.name}",
        )

    def repr_failure(self, excinfo):
        """Called by pytest to convert exceptions thrown in `runtest()` into
        pretty printer objects.

        """
        ex = excinfo.value
        assert isinstance(ex, MdTestFailure)
        return MdTestFailureRepr(self.checker, self.source, self.expected, ex.found)


def _empty_block(path: Path, source_last_line: int) -> Block:
    """If your code block doesn't have any output, we let you optionally
    omit the {testoutput} block after it.

    This function builds a fake, empty block that contains the
    expected empty output but with line numbers pointing at the end of
    the source block.

    """
    end_fence_line = source_last_line + 1
    return Block(path, "", end_fence_line, end_fence_line)


class MdTestFile(File):
    """pytest class used to discover all tests in a file."""

    def collect(self) -> Iterator[MdTestItem]:
        """Called by pytest to find all tests in this file.

        Read the Markdown file, then for each code block see if it's a
        Python block. If so, cache it to see if we find an output
        block. If we do, yield out a single test joining the source to
        the expected output in a `Block`. If we don't find an output
        block before the next code block, yield out a test expecting
        no output via `_empty_block`.

        """
        # Run all code blocks in this file in the same global
        # interpreter context. Pass this around to the inner calls to
        # `exec()` when actually running the code blocks.
        globs = {}

        tokens = myst_parser.main.to_tokens(self.fspath.read_text(encoding="UTF-8"))
        fences = (token for token in tokens if token.type == "fence")

        last_source = None

        def flush(source, expected):
            """Call this when we've paired up a bit of source Python with some
            expected output.

            That might happen when we find pairs of code-output or two
            code blocks in a row and we assume the optional output was
            missing.

            """
            assert (
                source
            ), f"Test output at {expected.path}:{expected.content_first_line} needs a proceeding code block"
            yield MdTestItem.from_parent(
                parent=self,
                globs=globs,
                source=source,
                expected=expected,
            )

        for token in fences:
            content_first_line, content_last_line = token.map
            # MyST puts the start as the line just before the opening
            # fence so bump that to the first line in the fence.
            content_first_line += 2
            # MyST puts the end as the line with the fence, so bump
            # back to the last line inside the fence.
            content_last_line -= 1
            if token.info.startswith("py"):
                # {testoutput} is optional. If there's no output
                # block, yield a test with empty output.
                if last_source is not None:
                    yield from flush(
                        last_source,
                        _empty_block(self.path, last_source.content_last_line),
                    )
                    last_source = None
                last_source = Block(
                    self.path, token.content, content_first_line, content_last_line
                )
            elif token.info.startswith("{testoutput}"):
                expected = Block(
                    self.path, token.content, content_first_line, content_last_line
                )
                yield from flush(last_source, expected)
                last_source = None

        # Emit that last test if there's no output block.
        if last_source is not None:
            yield from flush(
                last_source, _empty_block(self.path, last_source.content_last_line)
            )
            last_source = None


def pytest_collect_file(file_path, path: Path, parent) -> MdTestFile:
    """Hook function that pytest will call with each file found in this
    subdirectory.

    If we find a Markdown file, build a set of tests for it.

    """
    if file_path.suffix == ".md":
        return MdTestFile.from_parent(parent, path=file_path)
