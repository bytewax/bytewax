"""Connectors for local text files.

"""
from pathlib import Path

from bytewax.inputs import CustomPartInput


def _stateful_read(path, resume_i):
    with open(path) as f:
        for i, line in enumerate(f):
            # Resume to one after the last completed read.
            if i <= resume_i:
                continue
            yield i, line.strip()


class DirInput(CustomPartInput):
    """Read all files in a filesystem directory line-by-line.

    The directory must exist and contain identical data on all
    workers, so either run on a single machine or use a shared mount.

    Individual files are the unit of parallelism. Thus, lines from
    different files are interleaved.

    Args:

        dir: Path to directory.

        glob_pat: Pattern of files to read from the
            directory. Defaults to `"*"` or all files.

    """

    def __init__(self, dir: Path, glob_pat: str = "*"):
        self.dir = dir
        self.glob_pat = glob_pat

    def list_parts(self):
        return [
            str(path.relative_to(self.dir)) for path in self.dir.glob(self.glob_pat)
        ]

    def build_part(self, for_part, resume_state):
        path = self.dir / for_part
        resume_i = resume_state or -1

        return _stateful_read(path, resume_i)


class FileInput(CustomPartInput):
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
        return [str(self.path)]

    def build_part(self, for_part, resume_state):
        # TODO: Warn and return None. Then we could support
        # continuation from a different file.
        assert for_part == str(self.path), "Can't resume from different file"
        resume_i = resume_state or -1

        return _stateful_read(self.path, resume_i)
