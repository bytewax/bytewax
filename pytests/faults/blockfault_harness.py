"""Helpers for the block-fault gates in `test_block_faults.py`.

The device itself is built and faulted by `scripts/blockfault/env.sh`;
this module drives it, runs the workload in `flow_driver.py` as a
subprocess, and checks the output. See `scripts/blockfault/README.md`.

"""

import os
import pickle
import pickletools
import re
import signal
import sqlite3
import struct
import subprocess
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pytest

REPO = Path(__file__).resolve().parents[2]
ENV_SH = REPO / "scripts" / "blockfault" / "env.sh"
DRIVER = Path(__file__).with_name("flow_driver.py")
sys.path.insert(0, str(DRIVER.parent))

from flow_driver import TOTAL_BASE, expected_total  # noqa: E402

BS = 4096
# SQLite reports a write or fsync failure as "disk I/O error", but
# recent versions map EIO on a *read* to SQLITE_IOERR_CORRUPTFS, which
# reaches the application as SQLITE_CORRUPT: "database disk image is
# malformed". Both are the storage error a fault must end in.
STORAGE_ERROR = re.compile(
    r"disk I/O error|Input/output error|database disk image is malformed"
)


def env_run(*args: str) -> subprocess.CompletedProcess:
    """Run an `env.sh` command and return the result without checking it."""
    return subprocess.run(
        ["bash", str(ENV_SH), *args], capture_output=True, text=True, check=False
    )


def env_sh(*args: str, check: bool = True) -> str:
    """Run an `env.sh` command and return its output."""
    proc = env_run(*args)
    if check and proc.returncode != 0:
        msg = f"env.sh {' '.join(args)} failed ({proc.returncode}): {proc.stderr}"
        raise RuntimeError(msg)
    return proc.stdout


def require_target(target: str) -> None:
    """Skip, or fail when the run requires it, if a dm target is missing."""
    have = {line.split()[0] for line in env_sh("targets").splitlines() if line}
    if target in have:
        return
    required = os.environ.get("BLOCKFAULT_REQUIRE", "").split(",")
    msg = f"device-mapper target {target!r} is not available on this kernel"
    if target in required:
        pytest.fail(f"{msg}, and BLOCKFAULT_REQUIRE lists it")
    pytest.skip(msg)


@dataclass
class Device:
    """A mounted ext4 filesystem on a fault-injectable device."""

    name: str
    mnt: Path
    dir: Path
    dm: str

    @classmethod
    def _create(cls, *args: str) -> "Device":
        name = f"t{uuid.uuid4().hex[:8]}"
        out = env_sh(args[0], name, *args[1:])
        values = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
        return cls(name, Path(values["MNT"]), Path(values["DIR"]), values["DM"])

    @classmethod
    def up(cls) -> "Device":
        """Build and mount a new device."""
        return cls._create("up", "128")

    @classmethod
    def record_up(cls) -> "Device":
        """Build and mount a device whose every write is logged."""
        return cls._create("record-up", "128", "512")

    def down(self) -> None:
        """Tear the device down."""
        env_sh("down", self.name, check=False)

    def do(self, *args: str) -> str:
        """Run an `env.sh` command on this device."""
        return env_sh(args[0], self.name, *args[1:])

    def settle(self) -> None:
        """Write back everything dirty on the filesystem before a fault.

        Arming a write fault while older dirty data or an uncommitted
        journal transaction is pending makes the outcome depend on
        whether the background flusher or the next commit writes it.
        After `syncfs` the only writes that can hit the fault are the
        ones the next commit causes, so every run fails the same way.

        """
        subprocess.run(["sync", "-f", str(self.mnt)], check=True)

    def extents(self, path: Path) -> List[Tuple[int, int, int]]:
        """(logical block, physical block, length) of a file's extents."""
        subprocess.run(["sync", str(path)], check=True)
        out = subprocess.run(
            ["filefrag", "-v", f"-b{BS}", str(path)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        exts = []
        for line in out.splitlines():
            fields = line.replace("..", " ").replace(":", " ").split()
            if fields and fields[0].isdigit() and len(fields) >= 6:
                lstart, _lend, pstart, _pend, length = map(int, fields[1:6])
                exts.append((lstart, pstart, length))
        if not exts:
            msg = f"no extents for {path}: {out}"
            raise RuntimeError(msg)
        return exts

    def physical_block(self, path: Path, logical: int) -> int:
        """Physical block holding a file's logical block."""
        for lstart, pstart, length in self.extents(path):
            if lstart <= logical < lstart + length:
                return pstart + logical - lstart
        msg = f"logical block {logical} of {path} is not mapped"
        raise RuntimeError(msg)

    def device_offset(self, path: Path, file_offset: int) -> int:
        """Byte offset on the device of a byte in a file."""
        block = self.physical_block(path, file_offset // BS)
        return block * BS + file_offset % BS

    def file_blocks(self, path: Path) -> List[int]:
        """Every physical block of a file."""
        return [
            pstart + i
            for _lstart, pstart, length in self.extents(path)
            for i in range(length)
        ]

    def assert_unreadable(self, block: int) -> None:
        """Fail unless an O_DIRECT read of the block returns EIO."""
        result = self.do("probe", str(block)).strip()
        assert result == "eio", (
            f"block {block} reads fine: the fault is not armed, "
            "so this test would prove nothing"
        )


class Run:
    """One `flow_driver.py` execution in a subprocess."""

    def __init__(
        self,
        store: Path,
        out: Path,
        n: int,
        gate: Optional[Tuple[int, Path]] = None,
        marks: Optional[str] = None,
    ) -> None:
        """Start the workload.

        `gate` is (item, directory) for `--gate`; `marks` is the name of a
        dm-log-writes device to mark for `--marks`.

        """
        self.gate_dir = gate[1] if gate else None
        args = [sys.executable, str(DRIVER), str(store), str(out), str(n)]
        if gate:
            gate[1].mkdir(parents=True, exist_ok=True)
            args += ["--gate", str(gate[0]), str(gate[1])]
        if marks:
            args += ["--marks", marks]
        self.stderr_path = out.with_name(f"{out.name}.{uuid.uuid4().hex[:6]}.err")
        self._stderr = open(self.stderr_path, "w")
        self.proc = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=self._stderr
        )

    def stderr(self) -> str:
        """Everything the execution wrote to stderr so far."""
        self._stderr.flush()
        return self.stderr_path.read_text()

    def wait_gate(self, timeout: float = 120.0) -> None:
        """Wait until the source is parked at the gate."""
        assert self.gate_dir is not None
        deadline = time.monotonic() + timeout
        while not (self.gate_dir / "reached").exists():
            if self.proc.poll() is not None:
                code = self.proc.returncode
                msg = f"driver exited {code} before the gate:\n{self.stderr()}"
                raise AssertionError(msg)
            if time.monotonic() > deadline:
                self.proc.kill()
                msg = "driver never reached the gate"
                raise AssertionError(msg)
            time.sleep(0.02)

    def release(self) -> None:
        """Let the parked source continue."""
        assert self.gate_dir is not None
        (self.gate_dir / "release").touch()

    def kill(self) -> None:
        """SIGKILL the execution."""
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()

    def finish(self, timeout: float = 120.0) -> int:
        """Wait for the execution to exit and return its exit code."""
        try:
            return self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            msg = f"driver still running after {timeout} s:\n{self.stderr()}"
            raise AssertionError(msg) from None


@dataclass
class Outcome:
    """Items lost, lines with a wrong total, and replay counts."""

    missing: Set[int]
    wrong: List[Tuple[str, int, int]]
    dupes: Dict[int, int]


def read_output(out: Path) -> List[Tuple[str, int, int]]:
    """Parse the workload's output file."""
    if not out.exists():
        return []
    lines = []
    for line in out.read_text().splitlines():
        key, item, total = line.split(",")
        lines.append((key, int(item), int(total)))
    return lines


def check_output(out: Path, n: int) -> Outcome:
    """Zero loss, exactly-once state, and duplicates of the output."""
    lines = read_output(out)
    seen = Counter(item for _, item, _ in lines)
    return Outcome(
        missing=set(range(n)) - set(seen),
        wrong=[line for line in lines if line[2] != expected_total(line[1])],
        dupes={item: count - 1 for item, count in seen.items() if count > 1},
    )


def part_files(store: Path) -> List[Path]:
    """The store's partition files."""
    parts = sorted(store.glob("part-*.sqlite3"))
    assert parts, f"no recovery partitions in {store}"
    return parts


def checkpoint(store: Path) -> None:
    """Move every WAL into its main file, so the pages live there."""
    for part in part_files(store):
        con = sqlite3.connect(str(part))
        try:
            busy, _log, _ckpt = con.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            assert busy == 0, f"checkpoint of {part} was blocked"
        finally:
            con.close()


def table_pages(db: Path, names: Sequence[str]) -> Tuple[List[int], int]:
    """Page numbers holding the given tables or indexes, and the page size."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        marks = ",".join("?" for _ in names)
        try:
            rows = con.execute(
                f"SELECT pageno FROM dbstat WHERE name IN ({marks})", tuple(names)
            ).fetchall()
        except sqlite3.OperationalError as ex:
            pytest.fail(f"this sqlite3 build has no dbstat table: {ex}")
        (page_size,) = con.execute("PRAGMA page_size").fetchone()
    finally:
        con.close()
    return sorted({row[0] for row in rows}), page_size


def index_names(db: Path, table: str) -> List[str]:
    """A table's own name and the names of its indexes."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE tbl_name = ?", (table,)
        ).fetchall()
    finally:
        con.close()
    return [row[0] for row in rows]


def snapshot_int_offset(db: Path, state_key: str) -> Tuple[int, int]:
    """File offset of the integer inside a key's latest running total.

    Returns the offset and the total. The snapshot is the pickled
    total, stored as a BLOB; the offset points at the low byte of its
    4-byte little-endian integer.

    """
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT step_id, snap_epoch, ser_change FROM snaps WHERE state_key = ? "
            "ORDER BY snap_epoch DESC",
            (state_key,),
        ).fetchall()
    finally:
        con.close()
    for _step, _epoch, blob in rows:
        if blob is None:
            continue
        value = pickle.loads(blob)
        if isinstance(value, int) and value >= TOTAL_BASE:
            break
    else:
        msg = f"no running-total snapshot for key {state_key!r} in {db}"
        raise AssertionError(msg)
    payload = None
    for opcode, arg, pos in pickletools.genops(blob):
        if opcode.name == "BININT" and arg == value:
            assert pos is not None
            payload = pos + 1
    assert payload is not None, f"total {value} is not a BININT in {blob!r}"
    data = db.read_bytes()
    hits = [m.start() for m in re.finditer(re.escape(blob), data)]
    assert len(hits) == 1, f"snapshot bytes occur {len(hits)} times in {db}"
    return hits[0] + payload, value


def _wal_checksum(data: bytes, s0: int, s1: int, big: bool) -> Tuple[int, int]:
    words = struct.unpack(f"{'>' if big else '<'}{len(data) // 4}I", data)
    for i in range(0, len(words), 2):
        s0 = (s0 + words[i] + s1) & 0xFFFFFFFF
        s1 = (s1 + words[i + 1] + s0) & 0xFFFFFFFF
    return s0, s1


def wal_commit_frames(wal: bytes) -> List[int]:
    """Offsets of the page data of every valid commit frame in a WAL.

    Mirrors SQLite's recovery: frames count while their salts match
    the header and their cumulative checksums verify, and a frame
    whose header records a database size ends a transaction.

    """
    if len(wal) < 32:
        return []
    magic, _ver, page_size, _seq, salt1, salt2, c1, c2 = struct.unpack(">8I", wal[:32])
    big = bool(magic & 1)
    s0, s1 = _wal_checksum(wal[:24], 0, 0, big)
    assert (s0, s1) == (c1, c2), "WAL header checksum does not verify"
    commits = []
    off = 32
    while off + 24 + page_size <= len(wal):
        _pgno, db_size, fs1, fs2, fc1, fc2 = struct.unpack(">6I", wal[off : off + 24])
        if (fs1, fs2) != (salt1, salt2):
            break
        s0, s1 = _wal_checksum(wal[off : off + 8], s0, s1, big)
        s0, s1 = _wal_checksum(wal[off + 24 : off + 24 + page_size], s0, s1, big)
        if (s0, s1) != (fc1, fc2):
            break
        if db_size:
            commits.append(off + 24)
        off += 24 + page_size
    return commits
