"""Block-device fault gates for the recovery store.

These tests put a recovery store on a device that can fail on command
and check that every storage fault ends in one of two outcomes: a
deterministic failure that leaves the store as it was, or a correct
resume. Never a silent fresh start, a silently wrong state, or a hang.

They need root, the nbd and dm-mod kernel modules, and the tools in
`scripts/blockfault/Dockerfile`, so they only run inside
`scripts/blockfault/run.sh`, which sets `BYTEWAX_BLOCKFAULT=1`.
Elsewhere this module is skipped. Gates that need an optional
device-mapper target skip when it is missing, unless
`BLOCKFAULT_REQUIRE` names it; CI sets that so a kernel without the
target fails the job instead of passing it quietly.

Each gate checks its own precondition before it acts: an armed fault
must make an O_DIRECT read of a target block fail, and a corrupted
byte must be found exactly once. A test whose fault never fired would
otherwise pass while proving nothing.

The workload is `flow_driver.py`: N items, a running total per key,
and an output file outside the faulty device. Every output line
carries the total its key must have at that item, so a wrongly
restored state shows up in every later line of that key.

"""

import os
import sqlite3
from pathlib import Path
from typing import Iterator, List

import pytest

if os.environ.get("BYTEWAX_BLOCKFAULT") != "1":
    pytest.skip(
        "block-fault gates run only inside scripts/blockfault/run.sh",
        allow_module_level=True,
    )

from blockfault_harness import (  # noqa: E402
    BS,
    STORAGE_ERROR,
    Device,
    Run,
    check_output,
    checkpoint,
    index_names,
    part_files,
    read_output,
    require_target,
    snapshot_int_offset,
    table_pages,
    wal_commit_frames,
)
from bytewax.recovery import init_db_dir  # noqa: E402
from flow_driver import BATCH  # noqa: E402

N = 100
GATE = 50


@pytest.fixture
def device() -> Iterator[Device]:
    dev = Device.up()
    try:
        yield dev
    finally:
        dev.down()


@pytest.fixture
def store(device: Device) -> Path:
    path = device.mnt / "store"
    path.mkdir()
    init_db_dir(str(path), 1)
    return path


def crash_at_gate(store: Path, out: Path, gate_dir: Path) -> None:
    """Run until the gate, then SIGKILL: the store is durable to GATE."""
    run = Run(store, out, N, gate=(GATE, gate_dir))
    run.wait_gate()
    run.kill()
    emitted = sorted(item for _, item, _ in read_output(out))
    assert emitted == list(range(GATE)), "the store is not durable exactly to the gate"


def assert_fails_cleanly(run: Run, out: Path, lines_before: int) -> None:
    """The faulted execution fails with an I/O error and emits nothing."""
    code = run.finish()
    stderr = run.stderr()
    assert code != 0, f"execution succeeded over a faulty store:\n{stderr}"
    assert STORAGE_ERROR.search(stderr), f"failed without a storage error:\n{stderr}"
    assert len(read_output(out)) == lines_before, "the failed execution emitted output"


def assert_exact_resume(out: Path, allowed_dupes: List[int]) -> None:
    outcome = check_output(out, N)
    assert outcome.missing == set(), f"lost items: {sorted(outcome.missing)}"
    assert outcome.wrong == [], f"wrong restored state: {outcome.wrong[:5]}"
    unexpected = set(outcome.dupes) - set(allowed_dupes)
    assert (
        not unexpected
    ), f"replayed items outside the allowed window: {sorted(unexpected)}"


def store_data_blocks(device: Device, store: Path) -> List[int]:
    blocks = sorted(b for part in part_files(store) for b in device.file_blocks(part))
    assert blocks, "the store has no data blocks"
    return blocks


@pytest.mark.parametrize("backend", ["dust", "errseg", "nbdkit"])
def test_resume_over_unreadable_store(
    backend: str, device: Device, store: Path, tmp_path: Path
) -> None:
    """Latent read errors under the whole store fail the resume cleanly.

    `dust` models bad sectors (reads fail, writes would heal them),
    `errseg` maps the same blocks to device-mapper `error` segments,
    and `nbdkit` fails every read at the storage server.

    """
    if backend == "dust":
        require_target("dust")
    out = tmp_path / "out.csv"
    crash_at_gate(store, out, tmp_path / "gate")
    checkpoint(store)
    blocks = store_data_blocks(device, store)

    device.do("remount")
    # Warm the directory and inode blocks, so the read errors hit the
    # store's pages rather than its metadata.
    os.listdir(store)
    for part in part_files(store):
        part.stat()
    if backend == "dust":
        device.do("dust", *map(str, blocks))
    elif backend == "errseg":
        device.do("errseg", *map(str, blocks))
    else:
        device.do("nbd-fail", "read")
    device.assert_unreadable(blocks[0])

    assert_fails_cleanly(Run(store, out, N), out, GATE)

    device.do("heal")
    device.do("remount")
    assert Run(store, out, N).finish() == 0
    # The failed execution moved nothing: resume starts at the gate.
    assert_exact_resume(out, allowed_dupes=[])


@pytest.mark.parametrize("backend", ["dust", "errseg"])
def test_resume_over_unreadable_snapshot_pages(
    backend: str, device: Device, store: Path, tmp_path: Path
) -> None:
    """Bad blocks under only the snapshot pages fail the resume cleanly.

    The rest of the store reads fine, so the execution gets as far as
    loading state before it hits the error. It must still fail rather
    than resume with part of its state, and it must leave the resume
    point where it was.

    """
    if backend == "dust":
        require_target("dust")
    out = tmp_path / "out.csv"
    crash_at_gate(store, out, tmp_path / "gate")
    checkpoint(store)
    (part,) = part_files(store)
    pages, page_size = table_pages(part, index_names(part, "snaps"))
    assert page_size == BS, f"page size {page_size} is not the block size"
    assert 1 not in pages, "the snapshot table shares the header page"
    blocks = [device.physical_block(part, page - 1) for page in pages]
    readable = device.physical_block(part, 0)

    device.do("remount")
    os.listdir(store)
    part.stat()
    device.do(backend, *map(str, blocks))
    device.assert_unreadable(blocks[0])
    assert device.do("probe", str(readable)).strip() == "ok"

    assert_fails_cleanly(Run(store, out, N), out, GATE)

    device.do("heal")
    device.do("remount")
    assert Run(store, out, N).finish() == 0
    assert_exact_resume(out, allowed_dupes=[])


def test_write_error_at_boundary_commit(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """A write error while committing an epoch fails the execution.

    Writes start failing right before an epoch's recovery data is
    committed, so SQLite's fsync returns EIO. The execution must fail,
    and the next one must resume from the last boundary that did
    commit: the batch whose commit failed is replayed, nothing else.

    """
    out = tmp_path / "out.csv"
    gate_dir = tmp_path / "gate"
    run = Run(store, out, N, gate=(GATE, gate_dir))
    run.wait_gate()
    device.settle()
    device.do("nbd-fail", "write")
    run.release()
    code = run.finish()
    stderr = run.stderr()
    assert code != 0, f"execution succeeded while every write failed:\n{stderr}"
    assert STORAGE_ERROR.search(stderr), f"failed without a storage error:\n{stderr}"

    device.do("heal")
    device.do("fsck")
    assert Run(store, out, N).finish() == 0
    assert_exact_resume(out, allowed_dupes=list(range(GATE, GATE + BATCH)))


def test_storage_stall_fails_within_timeout(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """A storage server that stops answering ends in a bounded failure.

    nbdkit is stopped with SIGSTOP right before a commit, like a
    network volume that hangs. The nbd request timeout (5 s here) turns
    the hang into EIO; the execution must then fail instead of hanging,
    and resume correctly once the storage is reattached.

    """
    out = tmp_path / "out.csv"
    gate_dir = tmp_path / "gate"
    run = Run(store, out, N, gate=(GATE, gate_dir))
    run.wait_gate()
    device.settle()
    device.do("stall")
    run.release()
    code = run.finish(timeout=90)
    stderr = run.stderr()
    assert code != 0, f"execution succeeded on stalled storage:\n{stderr}"
    assert STORAGE_ERROR.search(stderr), f"failed without a storage error:\n{stderr}"

    device.do("restart")
    assert Run(store, out, N).finish() == 0
    assert_exact_resume(out, allowed_dupes=list(range(GATE, GATE + BATCH)))


def test_dropped_writes_replay_from_last_real_boundary(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """A disk that acknowledges writes and drops them costs replay only.

    From the gate on, dm-flakey acknowledges every write and discards
    it, like a drive that lies about its cache, and the execution runs
    to completion believing every commit durable. After a power cycle
    (cold cache over the real disk) the store is as it was at the
    gate: the next execution must resume there with the right state
    and replay what was lost, never resume from a mix of old and new.

    """
    require_target("flakey")
    out = tmp_path / "out.csv"
    gate_dir = tmp_path / "gate"
    run = Run(store, out, N, gate=(GATE, gate_dir))
    run.wait_gate()
    device.settle()
    device.do("drop-writes")
    run.release()
    assert run.finish() == 0, run.stderr()
    assert len(read_output(out)) == N

    # Power cycle: unmount through the lying disk, then come back up on
    # the real one with nothing cached.
    device.do("umount")
    device.do("heal")
    device.do("fsck")
    assert Run(store, out, N).finish() == 0
    assert_exact_resume(out, allowed_dupes=list(range(GATE, N)))
    outcome = check_output(out, N)
    assert set(outcome.dupes) == set(
        range(GATE, N)
    ), "the replay did not start at the gate"


def test_corrupt_wal_frame_rolls_back(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """A corrupted WAL frame loses its transaction, not the state.

    SQLite checksums WAL frames. Flipping a byte in the last committed
    frame on disk makes recovery stop before that transaction, so the
    store resumes from the boundary before it and replays one batch.

    """
    out = tmp_path / "out.csv"
    crash_at_gate(store, out, tmp_path / "gate")
    (part,) = part_files(store)
    wal = part.with_name(part.name + "-wal")
    assert wal.exists(), "the killed execution left no WAL"
    commits = wal_commit_frames(wal.read_bytes())
    assert len(commits) >= 2, "the WAL holds fewer than two transactions"
    target = device.device_offset(wal, commits[-1] + 100)

    device.do("umount")
    device.do("flip", str(target))
    device.do("mount")
    assert len(wal_commit_frames(wal.read_bytes())) == len(commits) - 1

    assert Run(store, out, N).finish() == 0
    assert_exact_resume(out, allowed_dupes=list(range(GATE - BATCH, GATE)))


class SilentCorruption(AssertionError):
    """A corrupted store resumed without error but with wrong state."""


@pytest.mark.xfail(
    raises=SilentCorruption,
    strict=True,
    reason="snapshot pages carry no checksum: a flipped bit in the store "
    "resumes silently with a wrong state",
)
def test_corrupt_snapshot_is_detected(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """A flipped bit in a snapshot must fail the resume, not alter state.

    The WAL is checksummed but the main database file is not, so a bit
    flipped in a snapshot row there (bit rot, a misdirected write, a
    firmware bug: anything the drive's own ECC does not catch) is read
    back without error. Resuming must then either fail
    deterministically or restore the right state; a running total that
    is off by one in every later line is neither.

    """
    out = tmp_path / "out.csv"
    crash_at_gate(store, out, tmp_path / "gate")
    checkpoint(store)
    (part,) = part_files(store)
    offset, total = snapshot_int_offset(part, "1")
    target = device.device_offset(part, offset)

    device.do("umount")
    device.do("flip", str(target))
    device.do("mount")
    assert snapshot_int_offset(part, "1")[1] == total ^ 0x01

    run = Run(store, out, N)
    if run.finish() != 0:
        return
    try:
        assert_exact_resume(out, allowed_dupes=[])
    except AssertionError as ex:
        raise SilentCorruption(str(ex)) from ex


class SilentFreshStart(AssertionError):
    """A store rolled back to its initial state resumed from scratch."""


@pytest.mark.xfail(
    raises=SilentFreshStart,
    strict=True,
    reason="a store its storage rolled back to the initial state is "
    "indistinguishable from a fresh one, so the next execution starts over",
)
def test_rolled_back_store_is_detected(
    device: Device, store: Path, tmp_path: Path
) -> None:
    """Losing the whole WAL must not look like a fresh store.

    Until its first checkpoint, all of a store's progress lives in the
    WAL. On ext4 without a journal, one write error on the WAL's
    extent-tree block can leave e2fsck no choice but to clear the file:
    these gates hit that about once in sixteen runs before the device
    was switched to a journaled filesystem. The store then holds exactly
    what `init_db_dir` wrote, and the next execution starts over from
    the first item without an error. Deleting the WAL while the store
    is closed reproduces that end state deterministically.

    """
    out = tmp_path / "out.csv"
    crash_at_gate(store, out, tmp_path / "gate")
    (part,) = part_files(store)
    wal = part.with_name(part.name + "-wal")
    assert wal.exists() and wal_commit_frames(
        wal.read_bytes()
    ), "no progress in the WAL"
    wal.unlink()
    part.with_name(part.name + "-shm").unlink(missing_ok=True)
    con = sqlite3.connect(f"file:{part}?mode=ro", uri=True)
    try:
        (executions,) = con.execute("SELECT count(*) FROM exs").fetchone()
    finally:
        con.close()
    assert executions == 0, "the main database file holds progress of its own"
    device.do("remount")

    run = Run(store, out, N)
    if run.finish() != 0:
        return
    replayed = sorted(check_output(out, N).dupes)
    if replayed and replayed[0] < GATE - BATCH:
        msg = f"resumed from item {replayed[0]} after the store lost its WAL"
        raise SilentFreshStart(msg)
