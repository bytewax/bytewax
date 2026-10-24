"""Replay of dm-log-writes logs, for the power-cut gate.

dm-log-writes (drivers/md/dm-log-writes.c; the constants below are
from v6.8) logs each write that reaches the device in the order it
became durable. Writes wait until a flush, which carries them into the
log with its own entry, and a FUA write is logged at once. So the state
after an entry with the FLUSH or FUA flag is one the device could hold
after a power cut at that point, and a prefix of the log ends there.

Layout, in units of the data device's logical sector (the super
block's `sectorsize`): sector 0 holds the super block. Each entry is
one sector holding a `log_write_entry`, and a write's data follows it
in the next `nr_sectors` sectors. A mark's text sits inside its entry
sector, and a discard has no data.

"""

import ctypes
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence

MAGIC = 0x6A736677736872
VERSION = 1
FLUSH = 1 << 0
FUA = 1 << 1
DISCARD = 1 << 2
MARK = 1 << 3
END_MARK = "dm-log-writes-end"

_SUPER = struct.Struct("<QQQI")  # magic, version, nr_entries, sectorsize
_ENTRY = struct.Struct("<QQQQ")  # sector, nr_sectors, flags, data_len
_FALLOC_KEEP_SIZE = 0x01
_FALLOC_PUNCH_HOLE = 0x02


@dataclass(frozen=True)
class Entry:
    """One log entry: a write, a discard, a flush or a mark."""

    index: int
    sector: int
    nr_sectors: int
    flags: int
    data_at: int
    data_bytes: int
    text: Optional[str]

    @property
    def is_point(self) -> bool:
        """Whether the state after this entry is a power-cut point."""
        return bool(self.flags & (FLUSH | FUA))


class Log:
    """A dm-log-writes log, parsed once."""

    def __init__(self, path: Path) -> None:
        """Parse the log at `path`; raises ValueError if it is malformed."""
        self.path = path
        self.entries: List[Entry] = []
        size = path.stat().st_size
        with open(path, "rb") as f:
            raw = f.read(_SUPER.size)
            if len(raw) < _SUPER.size:
                msg = f"{path} is too short for a dm-log-writes super block"
                raise ValueError(msg)
            magic, version, nr_entries, sectorsize = _SUPER.unpack(raw)
            if magic != MAGIC or version != VERSION:
                msg = f"{path} is not a dm-log-writes log (magic {magic:#x})"
                raise ValueError(msg)
            if sectorsize < 512 or sectorsize & (sectorsize - 1):
                msg = f"{path}: bad sector size {sectorsize}"
                raise ValueError(msg)
            self.sectorsize = sectorsize
            self.super_entries = nr_entries
            off = sectorsize
            while off + _ENTRY.size <= size:
                f.seek(off)
                head = f.read(_ENTRY.size)
                if head == bytes(_ENTRY.size):
                    break  # never written: the log ends here
                sector, nr, flags, data_len = _ENTRY.unpack(head)
                text = None
                data_at = off + sectorsize
                data_bytes = 0
                if flags & MARK:
                    f.seek(off + _ENTRY.size)
                    text = f.read(data_len).decode()
                    data_at = off + _ENTRY.size
                elif not flags & DISCARD:
                    data_bytes = nr * sectorsize
                end = data_at + data_bytes if data_bytes else off + sectorsize
                if end > size:
                    msg = f"{path}: entry {len(self.entries)} runs past the end"
                    raise ValueError(msg)
                self.entries.append(
                    Entry(
                        index=len(self.entries),
                        sector=sector,
                        nr_sectors=nr,
                        flags=flags,
                        data_at=data_at,
                        data_bytes=data_bytes,
                        text=text,
                    )
                )
                off = end

    def complete(self) -> bool:
        """Whether the log ends with the end mark and its count is current.

        dm-log-writes writes the end mark when its device is removed, and
        a log that ran out of space stops before that. The super block
        counts entries up to the last mark or FUA entry, so after the end
        mark it must equal the number of entries parsed.

        """
        return (
            bool(self.entries)
            and self.entries[-1].text == END_MARK
            and self.super_entries == len(self.entries)
        )

    def points(self) -> List[int]:
        """Indices of the entries that are power-cut points."""
        return [e.index for e in self.entries if e.is_point]

    def first_marks(self) -> Dict[str, int]:
        """The index of the first entry with each mark label."""
        first: Dict[str, int] = {}
        for entry in self.entries:
            if entry.text is not None:
                first.setdefault(entry.text, entry.index)
        return first


def _punch(fd: int, offset: int, length: int) -> None:
    """Zero a byte range of an image file, as a discard does."""
    libc = ctypes.CDLL(None, use_errno=True)
    fallocate = libc.fallocate
    fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_long, ctypes.c_long]
    fallocate.restype = ctypes.c_int
    mode = _FALLOC_PUNCH_HOLE | _FALLOC_KEEP_SIZE
    if fallocate(fd, mode, offset, length) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


class Image:
    """A zero-filled image that a log's entries are replayed onto, in order.

    `apply_through` moves forward only, so one image can serve every
    point in increasing order: copy it at a point, and the original keeps
    the state of the log up to that point.

    """

    def __init__(self, log: Log, path: Path, size: int) -> None:
        """Create a zero image of `size` bytes at `path`, replaying `log`."""
        with open(path, "wb") as f:
            f.truncate(size)
        self.log = log
        self.path = path
        self.size = size
        self._applied = 0
        self._image = os.open(path, os.O_RDWR)
        self._log = os.open(log.path, os.O_RDONLY)

    def apply_through(self, index: int) -> None:
        """Replay entries up to and including `index`."""
        if index < self._applied:
            msg = f"replay cannot go back from entry {self._applied} to {index}"
            raise ValueError(msg)
        for entry in self.log.entries[self._applied : index + 1]:
            self._apply(entry)
        self._applied = index + 1

    def _apply(self, entry: Entry) -> None:
        if entry.flags & MARK or not entry.nr_sectors:
            return
        unit = self.log.sectorsize
        offset = entry.sector * unit
        length = entry.nr_sectors * unit
        if offset + length > self.size:
            msg = f"entry {entry.index} writes past the end of the device"
            raise ValueError(msg)
        if entry.flags & DISCARD:
            _punch(self._image, offset, length)
            return
        data = os.pread(self._log, length, entry.data_at)
        if len(data) != length:
            msg = f"entry {entry.index}: log data is short"
            raise ValueError(msg)
        os.pwrite(self._image, data, offset)

    def close(self) -> None:
        """Close both files."""
        os.close(self._image)
        os.close(self._log)


def select_points(points: Sequence[int], boundary: int, budget: int) -> List[int]:
    """The points to replay: all of them, or a deterministic sample.

    With more points than `budget`, the first and the last are kept, and so
    is every point after `boundary` (the index of the last durable
    boundary), since the final states of a run live there. The rest is
    sampled at a fixed stride to fill the budget.

    """
    if len(points) <= budget:
        return list(points)
    keep = {points[0], points[-1]} | {p for p in points if p > boundary}
    head = [p for p in points if p <= boundary]
    room = budget - len(keep)
    if head and room > 0:
        stride = -(-len(head) // room)
        keep.update(head[::stride])
    return sorted(keep)
