"""Power-cut gate: the recovery store at every flush point of a run.

The whole run is recorded through dm-log-writes, which logs every write
that reaches the device, with its flushes, in durable order (see
`log_writes.py`). Every point where the log says a power cut leaves a
durable state, an entry with FLUSH or FUA, is then replayed: the log
prefix goes onto a zeroed image, which is mounted (ext4 replays its
journal), checked, and the workload is resumed on it.

Points before the store is initialised (the `init` mark) are not a store
yet: such a point may fail to mount, or the store may refuse to resume,
as long as nothing is emitted. A point that does mount must still be
clean. From the `init` mark on, each point must satisfy:

- the filesystem is clean after its journal replay (`e2fsck -fn`), and
  the store resumes without a storage or SQLite error;
- no item is lost, and every output line carries the right running
  total (`flow_driver.expected_total`);
- the resume starts no earlier than the last boundary the point made
  durable, and no later than the batch after it: never a silent fresh
  start, and nothing replayed from before the boundary;
- only items at or after that boundary are emitted twice.

The boundary and the output lines on disk at a point come from marks
in the log (`flow_driver.py --marks`), not from the store, so a store
that forgot durable progress is caught here.

Only a Linux host with the dm-log-writes module has this target. Elsewhere
the gate skips with the reason, or fails when `BLOCKFAULT_REQUIRE` lists
`log-writes`.

"""

import filecmp
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

if os.environ.get("BYTEWAX_BLOCKFAULT") != "1":
    pytest.skip(
        "block-fault gates run only inside scripts/blockfault/run.sh",
        allow_module_level=True,
    )

from blockfault_harness import (  # noqa: E402
    Device,
    Run,
    check_output,
    env_run,
    env_sh,
    read_output,
    require_target,
)
from bytewax.recovery import init_db_dir  # noqa: E402
from flow_driver import BATCH  # noqa: E402
from log_writes import Image, Log, select_points  # noqa: E402

N = 100
# Replaying a point costs a copy of the image and a mount, so the sweep
# samples past this many points; the first, the last and every point
# after the last durable boundary are always replayed.
MAX_POINTS = 300
Line = Tuple[str, int, int]


def labelled(first: Dict[str, int], prefix: str) -> Dict[int, int]:
    """The index of the first mark of each `prefix<item>` label."""
    return {
        int(label[len(prefix) :]): index
        for label, index in first.items()
        if label.startswith(prefix)
    }


def boundary_at(boundaries: Dict[int, int], point: int) -> int:
    """The last boundary durable at a point.

    A boundary's mark is written after the commit that made it durable, so
    every boundary whose mark precedes the point is durable there. A commit
    can also finish just before the point while its mark comes after it;
    the resume may then start one batch further on, which is allowed.

    """
    return max((item for item, index in boundaries.items() if index < point), default=0)


def output_before(outputs: Dict[int, int], point: int) -> Set[int]:
    """The items whose output line was on disk before a point."""
    return {item for item, index in outputs.items() if index < point}


def fs_problems(image: Path) -> List[str]:
    """Problems `e2fsck -fn` finds in a cleanly unmounted image."""
    proc = subprocess.run(
        ["e2fsck", "-fn", str(image)], capture_output=True, text=True, check=False
    )
    if proc.returncode == 0:
        return []
    return [f"e2fsck -fn exits {proc.returncode}: {proc.stdout.strip()[-300:]}"]


def resume(
    dev: Device,
    point: int,
    recorded: List[Line],
    boundaries: Dict[int, int],
    outputs: Dict[int, int],
    work: Path,
    setup: bool,
) -> List[str]:
    """Resume the workload on the mounted image; return the violations."""
    k_lo = boundary_at(boundaries, point)
    prior = [line for line in recorded if line[1] in output_before(outputs, point)]
    out = work / "out.csv"
    out.write_text("".join(f"{key},{item},{total}\n" for key, item, total in prior))
    run = Run(dev.mnt / "store", out, N)
    code = run.finish()
    if code != 0:
        stderr = run.stderr().strip()[-400:]
        if setup and not read_output(out):
            return []  # refused before any output: nothing was started
        return [f"the store does not resume (exit {code}): {stderr}"]

    resumed = read_output(out)[len(prior) :]
    start = min((item for _, item, _ in resumed), default=N)
    problems = []
    if start == 0 and k_lo > 0:
        problems.append(
            f"silent fresh start: resumed from item 0 after boundary {k_lo} was durable"
        )
    elif start < k_lo:
        problems.append(
            f"resumed at item {start}, before the last durable boundary {k_lo}"
        )
    elif start > k_lo + BATCH:
        problems.append(
            f"resumed at item {start}, past the batch after boundary {k_lo}"
        )
    outcome = check_output(out, N)
    if outcome.missing:
        problems.append(f"lost items: {sorted(outcome.missing)[:10]}")
    if outcome.wrong:
        problems.append(f"wrong running totals: {outcome.wrong[:3]}")
    early = sorted(item for item in outcome.dupes if item < k_lo)
    if early:
        problems.append(f"items before boundary {k_lo} emitted twice: {early[:10]}")
    return problems


def replay_point(
    dev: Device,
    image: Path,
    point: int,
    recorded: List[Line],
    boundaries: Dict[int, int],
    outputs: Dict[int, int],
    work: Path,
    setup: bool,
) -> List[str]:
    """Mount a replayed image, check it, resume on it; return the violations."""
    mounted = env_run("replay-mount", dev.name, str(image))
    if mounted.returncode != 0:
        if setup:
            return []  # mkfs or the store's init is not finished: nothing to mount
        return [f"the image does not mount: {mounted.stderr.strip()}"]
    # Unmounting writes the replayed journal back, so the check sees the
    # filesystem as a clean unmount would leave it.
    env_sh("replay-umount", dev.name)
    problems = fs_problems(image)
    mounted = env_run("replay-mount", dev.name, str(image))
    if mounted.returncode != 0:
        return problems + [f"the image does not mount again: {mounted.stderr.strip()}"]
    try:
        problems += resume(dev, point, recorded, boundaries, outputs, work, setup)
    finally:
        env_sh("replay-umount", dev.name)
    return problems + fs_problems(image)


def test_power_cut_at_every_flush_point(tmp_path: Path) -> None:
    """A power cut at any flush point leaves a store that resumes exactly.

    The recording is a full run of the workload from a fresh filesystem,
    so the replay starts from zeros and rebuilds mkfs as well. Before the
    sweep the log must replay to exactly the device it recorded, or the
    replay proves nothing about the device.

    """
    require_target("log-writes")
    work = tmp_path / "replay"
    work.mkdir()
    dev = Device.record_up()
    try:
        store = dev.mnt / "store"
        store.mkdir()
        init_db_dir(str(store), 1)
        dev.do("mark", "init")
        recorded_out = tmp_path / "recorded.csv"
        run = Run(store, recorded_out, N, marks=dev.dm)
        assert run.finish(timeout=300) == 0, f"recording failed:\n{run.stderr()}"
        recorded = read_output(recorded_out)
        recorded_check = check_output(recorded_out, N)
        assert recorded_check.missing == set() and recorded_check.wrong == []
        assert recorded_check.dupes == {}, "the recorded run emitted duplicates"
        dev.do("record-stop")

        log = Log(dev.dir / "log.img")
        assert log.complete(), "the log is incomplete: it lacks its end mark"
        size = (dev.dir / "disk.img").stat().st_size
        full = Image(log, work / "full.img", size)
        full.apply_through(len(log.entries) - 1)
        full.close()
        assert filecmp.cmp(
            work / "full.img", dev.dir / "disk.img", shallow=False
        ), "the log does not replay to the recorded device"
        (work / "full.img").unlink()

        points = log.points()
        assert points, "the recording has no flush or FUA points"
        first = log.first_marks()
        boundaries = labelled(first, "b")
        outputs = labelled(first, "o")
        assert boundaries and outputs, "the recording carries no marks"
        init_at = first.get("init")
        assert init_at is not None, "the recording lacks the init mark"
        selected = select_points(points, max(boundaries.values()), MAX_POINTS)
        setup_count = sum(1 for point in selected if point < init_at)
        print(
            f"power cut: {len(selected)} of {len(points)} flush points replayed"
            f" ({setup_count} before the store was initialised)",
            flush=True,
        )

        base = Image(log, work / "base.img", size)
        violations: List[str] = []
        try:
            for point in selected:
                base.apply_through(point)
                image = work / "point.img"
                subprocess.run(
                    ["cp", "--sparse=always", str(base.path), str(image)],
                    check=True,
                )
                problems = replay_point(
                    dev,
                    image,
                    point,
                    recorded,
                    boundaries,
                    outputs,
                    work,
                    setup=point < init_at,
                )
                image.unlink()
                if problems:
                    violations.append(f"entry {point}: " + "; ".join(problems))
        finally:
            base.close()
        assert not violations, (
            f"{len(violations)} of {len(selected)} points violate the invariants:\n"
            + "\n".join(violations)
        )
    finally:
        dev.down()
