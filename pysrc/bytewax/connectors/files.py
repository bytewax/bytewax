"""Connectors for local text files.

"""
import os
from pathlib import Path
from typing import Callable, Union
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
        return [line]

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

    This file must exist and be identical on all workers.

    There is no parallelism; only one worker will actually read the
    file.

    Args:

        path: Path to file.

    """

    def __init__(self, path: Union[Path, str]):
        if not isinstance(path, Path):
            path = Path(path)
        self._path = path

    def list_parts(self):
        return {str(self._path)}

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume reading from different file"
        return _FileSource(self._path, resume_state)


class CSVInput(FileInput):
    """Read a single csv file line-by-line from the filesystem.

    Will read the first row as the header.

    For each successive line  it will return a dictionary
    with the header as keys like the DictReader() method.

    This csv file must exist and be identical on all workers.

    There is no parallelism; only one worker will actually read the
    file.

    Args:

        path: Path to file.
        **fmtparams: Any custom formatting arguments you can pass to [`csv.reader`](https://docs.python.org/3/library/csv.html?highlight=csv#csv.reader).

    sample input:

    ```
    index,timestamp,value,instance
    0,2022-02-24 11:42:08,0.132,24ae8d
    0,2022-02-24 11:42:08,0.066,c6585a
    0,2022-02-24 11:42:08,42.652,ac20cd
    0,2022-02-24 11:42:08,51.846,5f5533
    0,2022-02-24 11:42:08,2.296,fe7f93
    0,2022-02-24 11:42:08,1.732,53ea38
    0,2022-02-24 11:42:08,91.958,825cc2
    0,2022-02-24 11:42:08,0.068,77c1ca
    ```

    sample output:

    ```
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '0.132', 'instance': '24ae8d'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '0.066', 'instance': 'c6585a'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '42.652', 'instance': 'ac20cd'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '51.846', 'instance': '5f5533'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '2.296', 'instance': 'fe7f93'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '1.732', 'instance': '53ea38'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '91.958', 'instance': '825cc2'}
    {'index': '0', 'timestamp': '2022-02-24 11:42:08', 'value': '0.068', 'instance': '77c1ca'}
    ```
    """

    def __init__(self, path: Path, **fmtparams):
        super().__init__(path)
        self.fmtparams = fmtparams

    def build_part(self, for_part, resume_state):
        assert for_part == str(self._path), "Can't resume reading from different file"
        return _CSVSource(self._path, resume_state, **self.fmtparams)


class _CSVSource(_FileSource):
    """
    Handler for csv files to iterate line by line.
    Uses the csv reader assumes a header on the file
    on each next() call, will return a dict of header
    & values

    Called by CSVInput
    """

    def __init__(self, path, resume_state, **fmtparams):
        resume_offset = resume_state or 0
        self._f = open(path, "rt")
        self.fmtparams = fmtparams
        self.header = next(csv.reader([self._f.readline()], **self.fmtparams))
        if resume_offset:
            self._f.seek(resume_offset)

    def next(self):
        line = self._f.readline()
        csv_line = dict(zip(self.header, next(csv.reader([line], **self.fmtparams))))
        if len(line) <= 0:
            raise StopIteration()
        return [csv_line]

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
