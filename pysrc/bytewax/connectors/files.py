"""Connectors for local text files."""
import os
from csv import DictReader
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union
from zlib import adler32

from typing_extensions import override

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


def _get_path_dev(path: Path) -> str:
    return hex(path.stat().st_dev)


def _readlines(f) -> Iterator[str]:
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


def _strip_n(s: str) -> str:
    return s.rstrip("\n")


class _FileSourcePartition(StatefulSourcePartition[str, int]):
    def __init__(self, path: Path, batch_size: int, resume_state: Optional[int]):
        self._f = open(path, "rt")
        if resume_state is not None:
            self._f.seek(resume_state)
        it = map(_strip_n, _readlines(self._f))
        self._batcher = batch(it, batch_size)

    @override
    def next_batch(self) -> List[str]:
        return next(self._batcher)

    @override
    def snapshot(self) -> int:
        return self._f.tell()

    @override
    def close(self) -> None:
        self._f.close()


class DirSource(FixedPartitionedSource[str, int]):
    """Read all files in a filesystem directory line-by-line.

    The directory must exist on at least one worker. Each worker can
    have unique files at overlapping paths if each worker mounts a
    distinct filesystem. Tries to read only one instance of each
    unique file in the whole cluster by deduplicating paths by
    filesystem ID. See `get_fs_id` argument to adjust this.

    Unique files are the unit of parallelism; only one worker will
    read each unique file. Thus, lines from different files are
    interleaved.

    Can support exactly-once processing.

    """

    def __init__(
        self,
        dir_path: Path,
        glob_pat: str = "*",
        batch_size: int = 1000,
        get_fs_id: Callable[[Path], str] = _get_path_dev,
    ):
        """Init.

        :arg dir_path: Path to directory.

        :arg glob_pat: Pattern of files to read from the directory.
            Defaults to `"*"` or all files.

        :arg batch_size: Number of lines to read per batch. Defaults
            to 1000.

        :arg get_fs_id: Called with the directory and must return a
            consistent (across workers and restarts) unique ID for the
            filesystem of that directory. Defaults to using
            {py:obj}`os.stat_result.st_dev`.

            If you know all workers have access to identical files,
            you can have this return a constant: `lambda _dir:
            "SHARED"`.

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
        self._fs_id = get_fs_id(dir_path)
        if "::" in self._fs_id:
            msg = f"result of `get_fs_id` must not contain `::`; got {self._fs_id!r}"
            raise ValueError(msg)

    @override
    def list_parts(self) -> List[str]:
        if self._dir_path.exists():
            return [
                f"{self._fs_id}::{path.relative_to(self._dir_path)}"
                for path in self._dir_path.glob(self._glob_pat)
            ]
        else:
            return []

    @override
    def build_part(
        self, _step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _FileSourcePartition:
        _fs_id, for_path = for_part.split("::", 1)
        path = self._dir_path / for_path
        return _FileSourcePartition(path, self._batch_size, resume_state)


class FileSource(FixedPartitionedSource[str, int]):
    """Read a path line-by-line from the filesystem.

    The path must exist on at least one worker. Each worker can have a
    unique file at the path if each worker mounts a distinct
    filesystem. Tries to read only one instance of each unique file in
    the whole cluster by deduplicating paths by filesystem ID. See
    `get_fs_id` argument to adjust this.

    Unique files are the unit of parallelism; only one worker will
    read each unique file. Thus, lines from different files are
    interleaved.

    """

    def __init__(
        self,
        path: Union[Path, str],
        batch_size: int = 1000,
        get_fs_id: Callable[[Path], str] = _get_path_dev,
    ):
        """Init.

        :arg path: Path to file.

        :arg batch_size: Number of lines to read per batch. Defaults
            to 1000.

        :arg get_fs_id: Called with the parent directory and must
            return a consistent (across workers and restarts) unique
            ID for the filesystem of that directory. Defaults to using
            {py:obj}`os.stat_result.st_dev`.

            If you know all workers have access to identical files,
            you can have this return a constant: `lambda _dir:
            "SHARED"`.

        """
        if not isinstance(path, Path):
            path = Path(path)

        self._path = path
        self._batch_size = batch_size
        self._fs_id = get_fs_id(path.parent)
        if "::" in self._fs_id:
            msg = f"result of `get_fs_id` must not contain `::`; got {self._fs_id!r}"
            raise ValueError(msg)

    @override
    def list_parts(self) -> List[str]:
        if self._path.exists():
            return [f"{self._fs_id}::{self._path}"]
        else:
            return []

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _FileSourcePartition:
        _fs_id, path = for_part.split("::", 1)
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert path == str(self._path), "Can't resume reading from different file"
        return _FileSourcePartition(self._path, self._batch_size, resume_state)


class _CSVPartition(StatefulSourcePartition[Dict[str, str], int]):
    def __init__(
        self,
        path: Path,
        batch_size: int,
        resume_state: Optional[int],
        fmtparams: Dict[str, Any],
    ):
        self._f = open(path, "rt", newline="")
        reader = DictReader(_readlines(self._f), **fmtparams)
        # Force reading of the header.
        _ = reader.fieldnames
        if resume_state is not None:
            self._f.seek(resume_state)
        self._batcher = batch(reader, batch_size)

    @override
    def next_batch(self) -> List[Dict[str, str]]:
        return next(self._batcher)

    @override
    def snapshot(self) -> int:
        return self._f.tell()

    @override
    def close(self) -> None:
        self._f.close()


class CSVSource(FixedPartitionedSource[Dict[str, str], int]):
    """Read a path as a CSV file row-by-row as keyed-by-column dicts.

    The path must exist on at least one worker. Each worker can have a
    unique file at the path if each worker mounts a distinct
    filesystem. Tries to read only one instance of each unique file in
    the whole cluster by deduplicating paths by filesystem ID. See
    `get_fs_id` argument to adjust this.

    Unique files are the unit of parallelism; only one worker will
    read each unique file. Thus, lines from different files are
    interleaved.

    Sample input:

    ```
    index,timestamp,value,instance
    0,2022-02-24 11:42:08,0.132,24ae8d
    0,2022-02-24 11:42:08,0.066,c6585a
    0,2022-02-24 11:42:08,42.652,ac20cd
    ```

    Sample output:

    ```json
    {
        "index": "0",
        "timestamp": "2022-02-24 11:42:08",
        "value": "0.132",
        "instance": "24ae8d",
    }
    {
        "index": "0",
        "timestamp": "2022-02-24 11:42:08",
        "value": "0.066",
        "instance": "c6585a",
    }
    {
        "index": "0",
        "timestamp": "2022-02-24 11:42:08",
        "value": "42.652",
        "instance": "ac20cd",
    }
    ```
    """

    def __init__(
        self,
        path: Path,
        batch_size: int = 1000,
        get_fs_id: Callable[[Path], str] = _get_path_dev,
        **fmtparams,
    ):
        """Init.

        :arg path: Path to file.

        :arg batch_size: Number of lines to read per batch. Defaults
            to 1000.

        :arg get_fs_id: Called with the parent directory and must
            return a consistent (across workers and restarts) unique
            ID for the filesystem of that directory. Defaults to using
            {py:obj}`os.stat_result.st_dev`.

            If you know all workers have access to identical files,
            you can have this return a constant: `lambda _dir:
            "SHARED"`.

        :arg **fmtparams: Any custom formatting arguments you can pass
            to {py:obj}`csv.reader`.

        """
        self._file_source = FileSource(path, batch_size, get_fs_id)
        self._fmtparams = fmtparams

    @override
    def list_parts(self) -> List[str]:
        return self._file_source.list_parts()

    @override
    def build_part(self, step_id: str, for_part: str, resume_state: Optional[Any]):
        _fs_id, path = for_part.split("::", 1)
        assert path == str(
            self._file_source._path
        ), "Can't resume reading from different file"
        return _CSVPartition(
            self._file_source._path,
            self._file_source._batch_size,
            resume_state,
            self._fmtparams,
        )


class _FileSinkPartition(StatefulSinkPartition[str, int]):
    def __init__(self, path: Path, resume_state: Optional[int], end: str):
        resume_offset = 0 if resume_state is None else resume_state
        self._f = open(path, "at")
        self._f.seek(resume_offset)
        self._f.truncate()
        self._end = end

    @override
    def write_batch(self, values: List[str]) -> None:
        for value in values:
            self._f.write(value)
            self._f.write(self._end)
        self._f.flush()
        os.fsync(self._f.fileno())

    @override
    def snapshot(self) -> int:
        return self._f.tell()

    @override
    def close(self) -> None:
        self._f.close()


class DirSink(FixedPartitionedSink[str, int]):
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

        :arg dir_path: Path to directory.

        :arg file_count: Number of separate partition files to create.

        :arg file_namer: Will be called with two arguments, the file
            index and total file count, and must return the file name
            to use for that file partition. Defaults to naming files
            like `"part_{i}"`, where `i` is the file index.

        :arg assign_file: Will be called with the key of each consumed
            item and must return the file index the value will be
            written to. Will wrap to the file count if you return a
            larger value. Defaults to calling {py:obj}`zlib.adler32`
            as a simple globally-consistent hash.

        :arg end: String to write after each item. Defaults to
            newline.

        """
        self._dir_path = dir_path
        self._file_count = file_count
        self._file_namer = file_namer
        self._assign_file = assign_file
        self._end = end

    @override
    def list_parts(self) -> List[str]:
        return [self._file_namer(i, self._file_count) for i in range(self._file_count)]

    @override
    def part_fn(self, item_key: str) -> int:
        return self._assign_file(item_key)

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _FileSinkPartition:
        path = self._dir_path / for_part
        return _FileSinkPartition(path, resume_state, self._end)


class FileSink(FixedPartitionedSink[str, int]):
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

        :arg path: Path to file.

        :arg end: String to write after each item. Defaults to
            newline.

        """
        self._path = path
        self._end = end

    @override
    def list_parts(self) -> List[str]:
        return [str(self._path)]

    @override
    def part_fn(self, item_key: str) -> int:
        return 0

    @override
    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _FileSinkPartition:
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self._path), "Can't resume writing to different file"
        return _FileSinkPartition(self._path, resume_state, self._end)
