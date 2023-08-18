"""Connectors for local text files."""
import os
from csv import DictReader
from pathlib import Path
from typing import Callable, Union
from zlib import adler32

from bytewax.inputs import PartitionedInput, StatefulSource, batch
from bytewax.outputs import PartitionedOutput, StatefulSink

__all__ = [
    "CSVInput",
    "DirInput",
    "DirOutput",
    "FileInput",
    "FileOutput",
]


def _readlines(f):
    """Turn a file into a generator of lines but support `tell`.

    Python files don't support `tell` to learn the offset if you use
    them in iterator mode via `next`, so re-create that iterator using
    `readline`.

    """
    while True:
        line = f.readline()
        if len(line) <= 0:
            break
        yield line


def _strip_n(s):
    return s.rstrip("\n")


class _FileSource(StatefulSource):
    def __init__(self, path, batch_size, resume_state):
        self._f = open(path, "rt")
        if resume_state is not None:
            self._f.seek(resume_state)
        it = map(_strip_n, _readlines(self._f))
        self._batcher = batch(it, batch_size)

    def next_batch(self):
        return next(self._batcher)

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()


class DirInput(PartitionedInput):
    """Read all files in a filesystem directory line-by-line.

    The directory must exist on all workers. All unique file names
    within the directory across all workers will be read.

    Individual files are the unit of parallelism; only one worker will
    read each unique file name. Thus, lines from different files are
    interleaved.

    Can support exactly-once processing.

    """

    def __init__(self, dir_path: Path, glob_pat: str = "*", batch_size: int = 1000):
        """Init.

        Args:
            dir_path:
                Path to directory.
            glob_pat:
                Pattern of files to read from the directory. Defaults
                to `"*"` or all files.
            batch_size:
                Number of lines to read per batch. Defaults to 1000.

        """
        if not dir_path.exists():
            msg = f"input directory `{dir_path}` does not exist"
            raise ValueError(msg)
        if not dir_path.is_dir():
            msg = f"input directory `{dir_path}` is not a directory"
            raise ValueError(msg)

        self._dir_path = dir_path
        self._glob_pat = glob_pat
        self._batch_size = batch_size

    def list_parts(self):
        """Each file is a separate partition."""
        return [
            str(path.relative_to(self._dir_path))
            for path in self._dir_path.glob(self._glob_pat)
        ]

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        path = self._dir_path / for_part
        return _FileSource(path, self._batch_size, resume_state)


class FileInput(PartitionedInput):
    """Read a single file line-by-line from the filesystem.

    The file must exist on at least one worker.

    There is no parallelism; only one worker will actually read the
    file.
    """

    def __init__(self, path: Union[Path, str], batch_size: int = 1000):
        """Init.

        Args:
            path:
                Path to file.
            batch_size:
                Number of lines to read per batch. Defaults to 1000.

        """
        if not isinstance(path, Path):
            path = Path(path)

        self._path = path
        self._batch_size = batch_size

    def list_parts(self):
        """The file is a single partition."""
        if self._path.exists():
            return [str(self._path)]
        else:
            return []

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume reading from different file"
        return _FileSource(self._path, self._batch_size, resume_state)


class _CSVSource(StatefulSource):
    def __init__(self, path, batch_size, resume_state, fmtparams):
        self._f = open(path, "rt", newline="")
        reader = DictReader(_readlines(self._f), **fmtparams)
        # Force reading of the header.
        _ = reader.fieldnames
        if resume_state is not None:
            self._f.seek(resume_state)
        self._batcher = batch(reader, batch_size)

    def next_batch(self):
        return next(self._batcher)

    def snapshot(self):
        return self._f.tell()

    def close(self):
        self._f.close()


class CSVInput(FileInput):
    """Read a single CSV file row-by-row as keyed-by-column dicts.

    The CSV file must exist on at least one worker.

    There is no parallelism; only one worker will actually read the
    file.

    Sample input:

    ```
    index,timestamp,value,instance
    0,2022-02-24 11:42:08,0.132,24ae8d
    0,2022-02-24 11:42:08,0.066,c6585a
    0,2022-02-24 11:42:08,42.652,ac20cd
    ```

    Sample output:

    ```
    {
      'index': '0',
      'timestamp': '2022-02-24 11:42:08',
      'value': '0.132',
      'instance': '24ae8d'
    }
    {
      'index': '0',
      'timestamp': '2022-02-24 11:42:08',
      'value': '0.066',
      'instance': 'c6585a'
    }
    {
      'index': '0',
      'timestamp': '2022-02-24 11:42:08',
      'value': '42.652',
      'instance': 'ac20cd'
    }
    ```

    """

    def __init__(self, path: Path, batch_size: int = 1000, **fmtparams):
        """Init.

        Args:
            path:
                Path to file.
            batch_size:
                Number of lines to read per batch. Defaults to 1000.
            **fmtparams:
                Any custom formatting arguments you can pass to
                [`csv.reader`](https://docs.python.org/3/library/csv.html?highlight=csv#csv.reader).

        """
        super().__init__(path, batch_size)
        self._fmtparams = fmtparams

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        assert for_part == str(self._path), "Can't resume reading from different file"
        return _CSVSource(self._path, self._batch_size, resume_state, self._fmtparams)


class _FileSink(StatefulSink):
    def __init__(self, path, resume_state, end):
        resume_offset = 0 if resume_state is None else resume_state
        self._f = open(path, "at")
        self._f.seek(resume_offset)
        self._f.truncate()
        self._end = end

    def write_batch(self, items):
        for item in items:
            self._f.write(item)
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
    """

    def __init__(
        self,
        dir_path: Path,
        file_count: int,
        file_namer: Callable[[int, int], str] = lambda i, _n: f"part_{i}",
        assign_file: Callable[[str], int] = lambda k: adler32(k.encode()),
        end: str = "\n",
    ):
        """Init.

        Args:
            dir_path:
                Path to directory.
            file_count:
                Number of separate partition files to create.
            file_namer:
                Will be called with two arguments, the file index and
                total file count, and must return the file name to use
                for that file partition. Defaults to naming files like
                `"part_{i}"`, where `i` is the file index.
            assign_file:
                Will be called with the key of each consumed item and
                must return the file index the value will be written
                to. Will wrap to the file count if you return a larger
                value. Defaults to calling `zlib.adler32` as a simple
                globally-consistent hash.
            end:
                String to write after each item. Defaults to newline.

        """
        self._dir_path = dir_path
        self._file_count = file_count
        self._file_namer = file_namer
        self._assign_file = assign_file
        self._end = end

    def list_parts(self):
        """Each file is a partition."""
        return [self._file_namer(i, self._file_count) for i in range(self._file_count)]

    def part_fn(self, item_key):
        """Use the specified file assigner."""
        return self._assign_file(item_key)

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        path = self._dir_path / for_part
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
    """

    def __init__(self, path: Path, end: str = "\n"):
        """Init.

        Args:
            path:
                Path to file.
            end:
                String to write after each item. Defaults to newline.
        """
        self._path = path
        self._end = end

    def list_parts(self):
        """The file is a single partition."""
        return [str(self._path)]

    def part_fn(self, item_key):
        """Only one partition."""
        return 0

    def build_part(self, for_part, resume_state):
        """See ABC docstring."""
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume writing to different file"
        return _FileSink(self._path, resume_state, self._end)
