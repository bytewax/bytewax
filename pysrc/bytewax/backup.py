"""Backup to durable storage interface.

Subclass the `Backup` class to create an object that will be used by the dataflow
to manage the files used by the recovery system.

Bytewax makes some assumptions about the behavior of this storage:
- It's some sort of blob storage.
- Files are write once. They do not need to be modified in-place and can be immutable.
- Files can be deleted.
- Enables listing of files by name in something like a single directory or bucket.
  Sub-directories or any more hierarchy are not needed.
- File upload and deletion are atomic r.e. listing. Files either appear in the
  listing with full contents, or do not appear; no half-written files.
- Read-after-write consistency for listing.
"""

import logging
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Union

from typing_extensions import override

logger = logging.getLogger(__name__)


class Backup(ABC):
    """Backup to durable storage interface."""

    @abstractmethod
    def list_keys(self) -> List[str]:
        """List files in the storage."""
        ...

    @abstractmethod
    def upload(self, from_local: Path, to_key: str) -> None:
        """Upload the given file to durable storage under the name `to_key`."""
        ...

    @abstractmethod
    def download(self, from_key: str, to_local: Path) -> None:
        """Download the given key from the durable storage to a local path."""
        ...

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete the given key from durable storage.

        This is used for garbage collection/compaction.
        """
        ...


class FileSystemBackup(Backup):
    """Default Backup class, mainly intended for local execution and testing purposes.

    It moves files to and from the specified directories.
    """

    def __init__(self, path: Union[str, Path]):
        """Init and check that the directory exists."""
        if isinstance(path, str):
            self.path = Path(path)
        else:
            self.path = path
        assert self.path.exists(), f"Local state directory {self.path} doesn't exists!"

    @override
    def list_keys(self) -> List[str]:
        return [path.name for path in self.path.iterdir()]

    @override
    def upload(self, from_local: Union[str, Path], to_key: str) -> None:
        if isinstance(from_local, str):
            from_local = Path(from_local)
        assert from_local.exists(), f"Trying to upload non existing file: {from_local}!"
        dest = self.path / to_key
        shutil.move(from_local, dest)

    @override
    def download(self, from_key: str, to_local: Union[str, Path]) -> None:
        if isinstance(to_local, str):
            to_local = Path(to_local)
        assert (
            to_local.parent.exists()
        ), f"Trying to download to a non existing directory: {to_local}!"
        source = self.path / from_key
        shutil.copy(source, to_local)

    @override
    def delete(self, key: str) -> None:
        shutil.rmtree(self.path / key)


def file_system_backup(path: Union[str, Path, None] = None) -> FileSystemBackup:
    """Return an instanced FileSystemBackup object.

    Defaults to use a directory named `backup` in the same
    directory where the script is executed from ('os.getcwd()').
    The default directory still needs to be manually created.
    """
    if path is None:
        path = Path(os.getcwd()) / "backup"
    return FileSystemBackup(path)
