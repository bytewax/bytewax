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
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from typing_extensions import override

logger = logging.getLogger(__name__)


class Backup(ABC):
    """Backup to durable storage interface."""

    @abstractmethod
    def list_keys(self) -> List[str]:
        """List files in the storage."""
        ...

    @abstractmethod
    def upload(self, from_local: Path, to_key: str):
        """Upload the given file to durable storage under the name `to_key`."""
        ...

    @abstractmethod
    def download(self, from_key: str, to_local: Path):
        """Download the given key from the durable storage to a local path."""
        ...

    @abstractmethod
    def delete(self, key: str):
        """Delete the given key from durable storage.

        This is used for garbage collection/compaction.
        """
        ...


class NoopBackup(Backup):
    """NoopBackup.

    This class is here just to be used as a default.
    """

    @override
    def list_keys(self) -> List[str]:
        return []

    @override
    def upload(self, from_local: Path, to_key: str):
        pass

    @override
    def download(self, from_key: str, to_local: Path):
        pass

    @override
    def delete(self, key: str):
        pass
