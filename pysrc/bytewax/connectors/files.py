"""Connectors for local text files.

"""
import os
from pathlib import Path
from typing import Callable
from zlib import adler32
import csv

from bytewax.inputs import PartitionedInput, StatefulSource
from bytewax.outputs import PartitionedOutput, StatefulSink

__all__ = [
    "DirInput",
    "DirOutput",
    "FileInput",
    "FileOutput",
    "CSVInput",
]


class _FileSource(StatefulSource):
    def __init__(self, path, resume_state):
        resume_offset = resume_state or 0
        self._f = open(path, "rt")
        self._f.seek(resume_offset)

    def next(self):
        line = self._f.readline().rstrip("\n")
        if len(line) <= 0:
            raise StopIteration()
        return line

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()


class DirInput(PartitionedInput):
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

        self._dir = dir
        self._glob_pat = glob_pat

    def list_parts(self):
        return {
            str(path.relative_to(self._dir)) for path in self._dir.glob(self._glob_pat)
        }

    def build_part(self, for_part, resume_state):
        path = self._dir / for_part
        return _FileSource(path, resume_state)


class FileInput(PartitionedInput):
    """Read a single file line-by-line from the filesystem.

    FileInput will attempt to identify the type of the file.
    If the file is of type csv, it will return a dictionary
    with the headers as the keys.

    This file must exist and be identical on all workers.

    There is no parallelism; only one worker will actually read the
    file.

    Args:

        path: Path to file.

    """

    def __init__(self, path: Path):
        self._path = path
        if str(type(path)) == "<class 'pathlib.PosixPath'>":
            self.file_type = path.suffix
        else:
            self._path = Path(path)
            self.file_type = self._path.suffix
            

    def list_parts(self):
        return {str(self._path)}

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume reading from different file"
        if self.file_type == '.csv':
            return _CSVSource(self._path, resume_state)
        else:
            return _FileSource(self._path, resume_state)

class _CSVSource(StatefulSource):
    def __init__(self, path, resume_state):
        resume_offset = resume_state or 0
        self._f = open(path, "rt")
        self.header = list(csv.reader([self._f.readline()]))[0]
        header_position = self._f.tell()
        if resume_offset <= header_position:
            resume_offset = header_position
        self._f.seek(resume_offset)

    def next(self):
        line = self._f.readline()
        csv_line = dict(zip(self.header, list(csv.reader([line]))[0]))
        if len(line) <= 0:
            raise StopIteration()
        return csv_line

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()

class _FileSink(StatefulSink):
    def __init__(self, path, resume_state, end):
        resume_offset = resume_state or 0
        self._f = open(path, "at")
        self._f.seek(resume_offset)
        self._f.truncate()
        self._end = end

    def write(self, x):
        self._f.write(x)
        self._f.write(self._end)
        self._f.flush()
        os.fsync(self._f.fileno())

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()


class DirOutput(PartitionedOutput):
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
            to. Will wrap to the file count if you return a larger
            value. Defaults to calling `zlib.adler32` as a simple
            globally-consistent hash.

        end: String to write after each item. Defaults to newline.

    """

    def __init__(
        self,
        dir: Path,
        file_count: int,
        file_namer: Callable[[int, int], str] = lambda i, _n: f"part_{i}",
        assign_file: Callable[[str], int] = lambda k: adler32(k.encode()),
        end: str = "\n",
    ):
        self._dir = dir
        self._file_count = file_count
        self._file_namer = file_namer
        self._assign_file = assign_file
        self._end = end

    def list_parts(self):
        return {self._file_namer(i, self._file_count) for i in range(self._file_count)}

    def assign_part(self, item_key):
        i = self._assign_file(item_key) % self._file_count
        return self._file_namer(i, self._file_count)

    def build_part(self, for_part, resume_state):
        path = self._dir / for_part
        return _FileSink(path, resume_state, self._end)


class FileOutput(PartitionedOutput):
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

        end: String to write after each item. Defaults to newline.

    """

    def __init__(self, path: Path, end: str = "\n"):
        self._path = path
        self._end = end

    def list_parts(self):
        return {str(self._path)}

    def assign_part(self, item_key):
        return str(self._path)

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume writing to different file"
        return _FileSink(self._path, resume_state, self._end)
