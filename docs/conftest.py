"""Hooks and code to tell pytest how to run our Markdown documentation
as a kind of doctest.

Each file is a separate "script". Each Python code block within each
file will be run in sequence. If there's a code block tagged
`{testoutput}`, it is the **expected output** of the previous code
block, and will be compared to the stdout of the previous Python
code. Errors will be reported.

A Python code block can have a series of `doctest:FLAG_NAME` after the
language, which will enable the various [doctest option
flags](https://docs.python.org/3/library/doctest.html#option-flags)
when checking the output.

There are two new custom option flags:

- `SORT_OUTPUT` will sort the lines of the stdout before
  comparing them to the expected output.

- `SORT_EXPECTED` will sort the expected text before comparing them to
  the stdout of the previous code block

- `IGNORE_OUTPUT` will throw away all the stdout.

Note that the `multiprocessing` module (or spawning subprocesses in
general) won't always work because there's no plain Python main file
the subprocess can start. In general, for these doctest-style tests,
you should shim out process spawning with thread spawning; these
doctests are "examples of using the API" and real functionality should
be tested separately in a real pytest (where there are no limits on
using subprocesses).

"""
import doctest
import os
from contextlib import AbstractContextManager, redirect_stdout
from dataclasses import dataclass
from doctest import Example, OPTIONFLAGS_BY_NAME, OutputChecker, register_optionflag
from io import StringIO
from pathlib import Path
from traceback import print_exc
from typing import Any, Iterator

import myst_parser.main
from _pytest._code.code import ReprFileLocation, TerminalRepr
from _pytest._io import TerminalWriter
from pytest import File, Item

SORT_OUTPUT = register_optionflag("SORT_OUTPUT")
SORT_EXPECTED = register_optionflag("SORT_EXPECTED")
IGNORE_OUTPUT = register_optionflag("IGNORE_OUTPUT")


@dataclass
class Block:
    """Represents a code block in a Markdown file so we can reference it
    later.

    """

    path: Path
    content: str
    content_first_line: int
    content_last_line: int


@dataclass
class ExpectedOutput:
    """Represents the expected output for a test."""

    text: Block


class NoOutput(ExpectedOutput):
    """Represents when the expected output of a test should be empty.

    Pass the block containin the source code so that we can highlight
    something helpful if there's an error.

    """

    def __init__(self, source: Block):
        end_fence_line = source.content_last_line + 1
        empty_output = Block(source.path, "", end_fence_line, end_fence_line)
        super().__init__(empty_output)


def _sort_lines(s):
    return "".join(sorted(s.splitlines(keepends=True)))


class MdOutputChecker(OutputChecker):
    """Custom version of `OutputChecker` that supports our new flags."""

    def check_output(self, want, got, optionflags):
        if optionflags & IGNORE_OUTPUT:
            got = ""
        if optionflags & SORT_OUTPUT:
            got = _sort_lines(got)
        if optionflags & SORT_EXPECTED:
            want = _sort_lines(want)
        return super().check_output(want, got, optionflags)

    def output_difference(self, example, got, optionflags):
        if optionflags & IGNORE_OUTPUT:
            got = ""
        if optionflags & SORT_OUTPUT:
            got = _sort_lines(got)
        if optionflags & SORT_EXPECTED:
            example.want = _sort_lines(example.want)
        return super().output_difference(example, got, optionflags)


class MdTestFailure(Exception):
    """Exception thrown when the expected output doesn't match the found
    output.

    """

    def __init__(self, found: str):
        self.found = found


# Stolen from
# https://github.com/python/cpython/blob/c06a4ffe818feddef3b5083d9746a1c0b82c84ab/Lib/contextlib.yp#L761-L773
# which is only in Python 3.11.
class chdir(AbstractContextManager):
    """Temporarily cd into another directory."""

    def __init__(self, path):
        self.path = path
        self._old_cwd = []

    def __enter__(self):
        self._old_cwd.append(os.getcwd())
        os.chdir(self.path)

    def __exit__(self, *excinfo):
        os.chdir(self._old_cwd.pop())


@dataclass
class TestCode:
    """Represents a block of code to test and the checking flags."""

    source: Block
    globs: Any
    checker_flags: int = 0
    checker: OutputChecker = MdOutputChecker()

    def run(self, expected: ExpectedOutput):
        """`compile()` the source string, `exec()` it capturing all
        output for later comparison, and check that the output is
        correct.

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

        capture = StringIO()
        # Note that this will not capture output from subprocesses or
        # non-Python code. We hackily take advantage of this to
        # sometimes ignore Timely's debug stdout output.
        with redirect_stdout(capture):
            try:
                code = compile(lineno_adjusted_source, self.source.path, "exec")

                with chdir(self.source.path.parent):
                    # For some reason, if we're running something
                    # representing a module, there's only a globals
                    # dictionary. See
                    # https://docs.python.org/3/library/functions.html#exec
                    exec(code, self.globs, self.globs)
            except:  # noqa: E722
                print_exc(file=capture)

        found = capture.getvalue()
        expected = expected.text.content
        if not self.checker.check_output(expected, found, self.checker_flags):
            raise MdTestFailure(found)


class MdTestFailureRepr(TerminalRepr):
    """pytest class for pretty printing test results. Returned by
    `MdTestItem.repr_failure` to print nice things.

    """

    def __init__(self, code: TestCode, expected: ExpectedOutput, found: str):
        super().__init__()
        self.code = code
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
        digits = len(str(abs(self.code.source.content_last_line)))
        lines = self.code.source.content.splitlines()
        # Print the source that produced bad output.
        for offset, line in enumerate(lines):
            lineno = self.code.source.content_first_line + offset
            tw.line(f"{lineno:0>{digits}}    {line}")

        # Use the builtin doctest diff printer.
        example = Example(self.code.source.content, self.expected.text.content)
        tw.write(
            self.code.checker.output_difference(
                example, self.found, self.code.checker_flags
            )
        )

        # Write out the location of the expected output.
        expected_loc = ReprFileLocation(
            self.expected.text.path,
            self.expected.text.content_first_line,
            MdTestFailure.__name__,
        )
        expected_loc.toterminal(tw)


class MdTestItem(Item):
    """Represents a single test for pytest to run.

    There's one of these for every code block in the Markdown input
    (paired with an optional output block).

    """

    def __init__(self, *, code: TestCode, expected: ExpectedOutput, **kwargs):
        name = f"{expected.text.content_first_line}"
        super().__init__(name=name, **kwargs)
        self.code = code
        self.expected = expected

    def runtest(self):
        """Called by pytest to actually run the test!"""
        self.code.run(self.expected)

    def reportinfo(self):
        """Called by pytest before pretty printing out failures.

        Use this to show which code block failed.

        """
        return (
            self.expected.text.path,
            self.expected.text.content_first_line,
            f"[mdtest] {self.expected.text.path.name}:{self.name}",
        )

    def repr_failure(self, excinfo):
        """Called by pytest to convert exceptions thrown in `runtest()` into
        pretty printer objects.

        """
        ex = excinfo.value
        if isinstance(ex, MdTestFailure):
            return MdTestFailureRepr(self.code, self.expected, ex.found)
        else:
            return ex


def _parse_checker_flags(info_args):
    flags = 0
    for arg in info_args:
        if arg.startswith("doctest:"):
            name = arg[len("doctest:") :]  # noqa: E203
            if name not in OPTIONFLAGS_BY_NAME:
                raise ValueError(
                    f"unknown doctest flag {name!r}; "
                    f"options are {sorted(OPTIONFLAGS_BY_NAME.keys())}"
                )
            bit = OPTIONFLAGS_BY_NAME[name]
            flags |= bit
    return flags


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

        def build_item(code: TestCode, expected: ExpectedOutput):
            """Call this when we've paired up a bit of source Python with some
            expected output.

            That might happen when we find pairs of code-output or two
            code blocks in a row and we assume the optional output was
            missing.

            """
            assert code is not None, (
                "Test output at "
                f"{expected.text.path}:{expected.text.content_first_line} "
                "needs a proceeding code block"
            )
            if not code.checker_flags & doctest.SKIP:
                yield MdTestItem.from_parent(
                    parent=self,
                    code=code,
                    expected=expected,
                )

        last_code = None

        for token in fences:
            content_first_line, content_last_line = token.map
            # MyST puts the start as the line just before the opening
            # fence so bump that to the first line in the fence.
            content_first_line += 2
            # MyST puts the end as the line with the fence, so bump
            # back to the last line inside the fence.
            content_last_line -= 1

            block = Block(
                self.path, token.content, content_first_line, content_last_line
            )
            info = token.info.strip()
            info_args = info.split()[1:]

            if info.startswith("py"):
                # {testoutput} is optional. If there's no output
                # block, yield a test with empty output.
                if last_code is not None:
                    expected = NoOutput(last_code.source)
                    yield from build_item(last_code, expected)
                    last_code = None

                checker_flags = _parse_checker_flags(info_args)
                last_code = TestCode(block, globs, checker_flags)
            elif info.startswith("{testoutput}"):
                expected = ExpectedOutput(block)
                yield from build_item(last_code, expected)
                last_code = None

        # Emit that last test if there's no output block.
        if last_code is not None:
            expected = NoOutput(last_code.source)
            yield from build_item(last_code, expected)
            last_code = None


def pytest_collect_file(file_path, path: Path, parent) -> MdTestFile:
    """Hook function that pytest will call with each file found in this
    subdirectory.

    If we find a Markdown file, build a set of tests for it.

    """
    if file_path.suffix == ".md":
        return MdTestFile.from_parent(parent, path=file_path)
