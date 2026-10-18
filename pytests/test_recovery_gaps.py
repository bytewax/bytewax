"""Known recovery gaps, pinned as red tests.

Each test asserts the behavior the recovery store should have. The gap
it pins is still open, so the test is an expected failure: it is marked
`xfail` with the gap's own error as `raises`, and that error is raised
only at the gap, after every precondition has passed. Any other failure
fails the test for real. The marks are strict, so a closed gap turns
its test into a failure and the mark is removed together with the fix.

The workload runs in a subprocess, so a SIGKILL only ends that process.
Everything runs in plain temporary directories: no root, no block
devices, no broker.

"""

import pickle
import pickletools
import re
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from bytewax.recovery import init_db_dir
from pytest import mark

N = 100
GATE = 50
KEYS = 4
TOTAL_BASE = 1_000_000_000

# A flow whose source can be parked at a gate, so that the store is
# durable to GATE exactly when the test kills it: the source asks for
# its next batch only once the previous epoch is written. The running
# totals depend on each key's whole history, so a wrong restored state
# shows up in every later output line of that key. Output goes to a
# file outside the store.
_WORKLOAD = r"""
import os
import sys
import time
from datetime import timedelta

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.recovery import RecoveryConfig
from bytewax.testing import run_main

KEYS = 4
BATCH = 5
TOTAL_BASE = 1_000_000_000
store, out, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
gate = None
if len(sys.argv) > 4:
    gate = (int(sys.argv[4]), sys.argv[5])


class _Partition(StatefulSourcePartition):
    def __init__(self, resume, gate):
        self._next = 0 if resume is None else resume
        self._gate = gate

    def next_batch(self):
        if self._next >= count:
            raise StopIteration()
        if self._gate is not None and self._next >= self._gate[0]:
            gate_dir = self._gate[1]
            open(os.path.join(gate_dir, "reached"), "w").close()
            release = os.path.join(gate_dir, "release")
            while not os.path.exists(release):
                time.sleep(0.01)
            self._gate = None
        end = min(self._next + BATCH, count)
        batch = list(range(self._next, end))
        self._next = end
        return batch

    def snapshot(self):
        return self._next


class _Source(FixedPartitionedSource):
    def list_parts(self):
        return ["p0"]

    def build_part(self, step_id, for_part, resume_state):
        return _Partition(resume_state, gate)


class _FilePartition(StatelessSinkPartition):
    def __init__(self, path):
        self._file = open(path, "a")

    def write_batch(self, items):
        for key, (item, total) in items:
            self._file.write(f"{key},{item},{total}\n")
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self):
        self._file.close()


class _FileSink(DynamicSink):
    def build(self, step_id, worker_index, worker_count):
        return _FilePartition(out)


def _add(total, item):
    total = (TOTAL_BASE if total is None else total) + item
    return total, (item, total)


flow = Dataflow("recovery_gaps")
nums = op.input("inp", flow, _Source())
keyed = op.key_on("key", nums, lambda x: str(x % KEYS))
totals = op.stateful_map("sum", keyed, _add)
op.output("out", totals, _FileSink())
run_main(flow, epoch_interval=timedelta(0), recovery_config=RecoveryConfig(store))
"""


class SilentCorruption(AssertionError):
    """A corrupted snapshot resumed without error but with wrong state."""


class SilentFreshStart(AssertionError):
    """A store that lost its progress resumed from the first item."""


def _expected_total(item: int) -> int:
    """Running total of `item`'s key after `item` is added."""
    return TOTAL_BASE + sum(range(item % KEYS, item + 1, KEYS))


def _read_output(out: Path) -> List[Tuple[str, int, int]]:
    if not out.exists():
        return []
    lines = []
    for line in out.read_text().splitlines():
        key, item, total = line.split(",")
        lines.append((key, int(item), int(total)))
    return lines


class _Outcome:
    def __init__(self, out: Path) -> None:
        lines = _read_output(out)
        seen = Counter(item for _, item, _ in lines)
        self.missing: Set[int] = set(range(N)) - set(seen)
        self.dupes: Dict[int, int] = {i: c - 1 for i, c in seen.items() if c > 1}
        self.wrong = [line for line in lines if line[2] != _expected_total(line[1])]


def _new_store(tmp_path: Path) -> Path:
    store = tmp_path / "store"
    store.mkdir()
    init_db_dir(store, 1)
    return store


def _the_part(store: Path) -> Path:
    parts = sorted(store.glob("part-*.sqlite3"))
    assert len(parts) == 1, f"expected one recovery partition in {store}: {parts}"
    return parts[0]


def _spawn(
    store: Path, out: Path, log: Path, gate: Optional[Tuple[int, Path]] = None
) -> subprocess.Popen:
    args = [sys.executable, "-c", _WORKLOAD, str(store), str(out), str(N)]
    if gate is not None:
        gate[1].mkdir(parents=True, exist_ok=True)
        args += [str(gate[0]), str(gate[1])]
    # The child keeps its own copy of the file descriptor after `with`.
    with log.open("w") as err:
        return subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=err, cwd=str(store.parent)
        )


def _run_to_end(store: Path, out: Path, log: Path) -> int:
    proc = _spawn(store, out, log)
    try:
        return proc.wait(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        msg = f"execution still running after 120 s:\n{log.read_text()}"
        raise AssertionError(msg) from None


def _crash_at_gate(store: Path, out: Path, tmp_path: Path) -> None:
    """Run until the gate, then SIGKILL: the store is durable to GATE."""
    gate_dir = tmp_path / "gate"
    log = tmp_path / "crash.err"
    proc = _spawn(store, out, log, gate=(GATE, gate_dir))
    deadline = time.monotonic() + 120
    while not (gate_dir / "reached").exists():
        if proc.poll() is not None:
            stderr = log.read_text()
            msg = f"execution exited {proc.returncode} before the gate:\n{stderr}"
            raise AssertionError(msg)
        if time.monotonic() > deadline:
            proc.kill()
            proc.wait()
            msg = "execution never reached the gate"
            raise AssertionError(msg)
        time.sleep(0.02)
    proc.kill()
    proc.wait()
    emitted = sorted(item for _, item, _ in _read_output(out))
    assert emitted == list(range(GATE)), "the store is not durable exactly to the gate"


def _checkpoint(part: Path) -> None:
    """Move the WAL into the main file, so that the pages live there."""
    con = sqlite3.connect(str(part))
    try:
        busy, _log, _ckpt = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        con.close()
    assert busy == 0, f"checkpoint of {part} was blocked"


def _count(db: Path, sql: str) -> int:
    """Read-only count, which also sees rows still in the WAL."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        (value,) = con.execute(sql).fetchone()
    finally:
        con.close()
    return int(value)


def _snapshot_int_offset(db: Path, state_key: str) -> Tuple[int, int]:
    """File offset of the low byte of a key's latest running total, and the total.

    The snapshot is the pickled total, stored as a BLOB in the main file.
    Its 4-byte little-endian integer holds the total, so the offset points
    at the low byte. The blob must occur exactly once in the file.

    """
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "SELECT ser_change FROM snaps WHERE state_key = ? "
            "AND ser_change IS NOT NULL ORDER BY snap_epoch DESC",
            (state_key,),
        ).fetchall()
    finally:
        con.close()
    for (blob,) in rows:
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
    hits = [m.start() for m in re.finditer(re.escape(blob), db.read_bytes())]
    assert len(hits) == 1, f"snapshot bytes occur {len(hits)} times in {db}"
    return hits[0] + payload, value


def _flip_low_byte_bit(path: Path, offset: int) -> None:
    """Flip the lowest bit of the byte at `offset`, in place."""
    with path.open("r+b") as f:
        f.seek(offset)
        (byte,) = f.read(1)
        f.seek(offset)
        f.write(bytes([byte ^ 0x01]))


@mark.xfail(
    raises=SilentCorruption,
    strict=True,
    reason="snapshot pages carry no checksum: a flipped bit in a snapshot "
    "resumes silently with a wrong running total",
)
def test_corrupt_snapshot_is_detected(tmp_path: Path) -> None:
    """A flipped bit in a snapshot must fail the resume, not alter state.

    SQLite checksums WAL frames, but not the pages of the main database
    file, and the snapshots are raw pickles. So a bit flipped in a
    snapshot row there (bit rot, a misdirected write, a firmware bug the
    drive's own ECC does not catch) is read back without any error.
    Resuming must then either fail deterministically or restore the
    right state. A running total off by one in every later line is
    neither.

    """
    store = _new_store(tmp_path)
    out = tmp_path / "out.csv"
    _crash_at_gate(store, out, tmp_path)
    part = _the_part(store)
    _checkpoint(part)
    offset, total = _snapshot_int_offset(part, "1")

    _flip_low_byte_bit(part, offset)
    assert _snapshot_int_offset(part, "1")[1] == total ^ 1, "the flip did not take"

    if _run_to_end(store, out, tmp_path / "resume.err") != 0:
        # A deterministic failure is an acceptable outcome.
        return
    outcome = _Outcome(out)
    assert outcome.missing == set(), f"lost items: {sorted(outcome.missing)}"
    assert outcome.dupes == {}, f"replayed items: {sorted(outcome.dupes)}"
    if outcome.wrong:
        msg = f"exit 0 with wrong running totals, first: {outcome.wrong[:3]}"
        raise SilentCorruption(msg)


@mark.xfail(
    raises=SilentFreshStart,
    strict=True,
    reason="a store its storage rolled back to the initial state is "
    "indistinguishable from a fresh one, so the next execution starts over",
)
def test_lost_wal_is_not_a_fresh_start(tmp_path: Path) -> None:
    """Losing the WAL must not look like a fresh store.

    Until its first checkpoint, all of a store's progress lives in the
    WAL. If that file is lost (a crash that drops it, or a filesystem
    repair that clears it), the main file holds what `init_db_dir`
    wrote, and the next execution starts over from the first item
    without any error. A store rolled back to its initial state is a
    fresh store, up to the creation timestamp `init_db_dir` writes, so
    nothing inside the store can tell the two apart. Detection needs a
    witness outside it.

    """
    store = _new_store(tmp_path)
    out = tmp_path / "out.csv"
    _crash_at_gate(store, out, tmp_path)
    part = _the_part(store)
    wal = part.with_name(part.name + "-wal")
    shm = part.with_name(part.name + "-shm")
    assert wal.exists(), "the killed execution left no WAL"
    assert _count(part, "SELECT count(*) FROM snaps") > 0, "no progress in the WAL"
    wal.unlink()
    shm.unlink(missing_ok=True)
    assert (
        _count(part, "SELECT count(*) FROM exs") == 0
    ), "the main database file holds progress of its own"

    if _run_to_end(store, out, tmp_path / "resume.err") != 0:
        # A deterministic refusal is the desired outcome.
        return
    emitted = Counter(item for _, item, _ in _read_output(out))
    if emitted[0] > 1:
        msg = (
            "exit 0 after the WAL was lost: the execution started over at item 0 "
            f"(item 0 emitted {emitted[0]} times)"
        )
        raise SilentFreshStart(msg)
    outcome = _Outcome(out)
    assert outcome.missing == set(), f"lost items: {sorted(outcome.missing)}"
    assert outcome.dupes == {}, f"replayed items: {sorted(outcome.dupes)}"
    assert outcome.wrong == [], f"wrong running totals: {outcome.wrong[:3]}"
