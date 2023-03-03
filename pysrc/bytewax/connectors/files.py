"""Connectors for local text files.

"""
import os
from pathlib import Path
from typing import Callable

from bytewax.inputs import PartInput
from bytewax.outputs import PartOutput


def _stateful_read(path, resume_state):
    resume_i = resume_state or -1

    with open(path) as f:
        for i, line in enumerate(f):
            # Resume to one after the last completed read.
            if i <= resume_i:
                continue
            yield i, line.strip()


class DirInput(PartInput):
    """Read all files in a filesystem directory line-by-line.

    The directory must exist and contain identical data on all
    workers, so either run on a single machine or use a shared mount.

    Individual files are the unit of parallelism. Thus, lines from
    different files are interleaved.

    Can support exactly-once processing.

    Args:

        dir: Path to directory.

        glob_pat: Pattern of files to read from the
            directory. Defaults to `"*"` or all files.

    """

    def __init__(self, dir: Path, glob_pat: str = "*"):
        if not dir.exists():
            raise ValueError(f"input directory `{dir}` does not exist")
        if not dir.is_dir():
            raise ValueError(f"input directory `{dir}` is not a directory")

        self.dir = dir
        self.glob_pat = glob_pat

    def list_parts(self):
        return {
            str(path.relative_to(self.dir)) for path in self.dir.glob(self.glob_pat)
        }

    def build_part(self, for_part, resume_state):
        path = self.dir / for_part

        return _stateful_read(path, resume_state)


class FileInput(PartInput):
    """Read a single file line-by-line from the filesystem.

    This file must exist and be identical on all workers.

    There is no parallelism; only one worker will actually read the
    file.

    Args:

        path: Path to file.

    """

    def __init__(self, path: Path):
        self.path = path

    def list_parts(self):
        return {str(self.path)}

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self.path), "Can't resume reading from different file"

        return _stateful_read(self.path, resume_state)


def _stateful_write_builder(path, resume_state, end):
    resume_offset = resume_state or 0

    f = open(path, "a")
    f.seek(resume_offset)

    def write(x):
        f.write(x)
        f.write(end)
        f.flush()
        os.fsync(f.fileno())
        return f.tell()

    return write


class DirOutput(PartOutput):
    """Write to a set of files in a filesystem directory line-by-line.

    Items consumed from the dataflow must look like two-tuples of
    `(key, value)`, where the value must look like a string. Use a
    proceeding map step to do custom formatting.

    The directory must exist and contain identical data on all
    workers, so either run on a single machine or use a shared mount.

    Individual files are the unit of parallelism.

    Can support exactly-once processing in a batch context. Each file
    will be truncated during resume so duplicates are
    prevented. Tailing the output files will result in undefined
    behavior.

    Args:

        dir: Path to directory.

        file_count: Number of separate partition files to create.

        file_namer: Will be called with two arguments, the file index
            and total file count, and must return the file name to
            use for that file partition. Defaults to naming files like
            `"part_{i}"`, where `i` is the file index.

        assign_file: Will be called with the key of each consumed item
            and must return the file index the value will be written
            to. Defaults to calling `hash`.

        end: String to write after each item. Defaults to `"\n"`.

    """

    def __init__(
        self,
        dir: Path,
        file_count: int,
        file_namer: Callable[[int, int], str] = lambda i, _n: f"part_{i}",
        assign_file: Callable[[str], int] = hash,
        end: str = "\n",
    ):
        self.dir = dir
        self.file_count = file_count
        self.file_namer = file_namer
        self.assign_file = assign_file
        self.end = end

    def list_parts(self):
        return {self.file_namer(i, self.file_count) for i in range(self.file_count)}

    def assign_part(self, item_key):
        i = self.assign_file(item_key) % self.file_count
        return self.file_namer(i, self.file_count)

    def build_part(self, for_part, resume_state):
        path = self.dir / for_part

        return _stateful_write_builder(path, resume_state, self.end)


class FileOutput(PartOutput):
    """Write to a single file line-by-line on the filesystem.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    The file must exist and be identical on all workers.

    There is no parallelism; only one worker will actually write to
    the file.

    Can support exactly-once processing in a batch context. The file
    will be truncated during resume so duplicates are
    prevented. Tailing the output file will result in undefined
    behavior.

    Args:

        path: Path to file.

        end: String to write after each item. Defaults to `"\n"`.

    """

    def __init__(self, path: Path, end: str = "\n"):
        self.path = path
        self.end = end

    def list_parts(self):
        return {str(self.path)}

    def assign_part(self, item_key):
        return str(self.path)

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self.path), "Can't resume writing to different file"

        return _stateful_write_builder(self.path, resume_state, self.end)
