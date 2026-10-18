# Block-device fault gates

These gates put a bytewax recovery store on a block device that fails
on command, and check that every storage fault ends in one of two
outcomes:

- a **deterministic failure** that leaves the store as it was, so the
  next execution resumes exactly where the last good one stopped; or
- a **correct resume**: no lost input, the right restored state, and
  replay confined to what the fault cost.

Never a silent fresh start, a silently wrong state, or a hang.

## The stack

```
sparse image file
  -> nbdkit, error filter       userspace "storage server"
  -> /dev/nbdN                  kernel nbd client, request timeout
  -> device-mapper device       linear, or a fault target
  -> ext4, journaled            errors=continue
  -> recovery store directory
```

The power-cut gate puts dm-log-writes in place of the fault layers,
with its log on a loop device; see [Power cuts](#power-cuts).

`env.sh` builds and controls it. Each layer has its own faults:

| layer | fault | models |
|---|---|---|
| nbdkit | every read or write fails while a trigger file exists | a failing volume |
| nbdkit | SIGSTOP / SIGCONT the server | a network volume that hangs (EBS, Ceph RBD, iSCSI) |
| nbd | request timeout (`-t`, 5 s) | the kernel giving up on a hung volume |
| dm-dust | reads of chosen blocks fail; a write heals the block | latent bad sectors |
| dm `error` | reads and writes of chosen blocks fail | bad sectors, on any kernel |
| dm-flakey | writes are acknowledged and dropped | a drive that lies about its cache |
| dm-log-writes | the device is cut after any prefix of its flushed writes | a power cut, at each flush point (`log-writes` on a loop device) |
| raw device | one byte flipped while unmounted | silent corruption the drive's ECC missed |

## Determinism

Every gate must end the same way on every run. Two things make the
write-fault gates deterministic:

- **A journaled filesystem.** After a failed write, journal replay
  brings the metadata back to its last committed transaction, no
  matter which write failed. Without a journal the outcome depends on
  which write the fault caught: these gates first ran on ext4 without
  a journal, and about once in sixteen runs the fault landed on the
  WAL's extent-tree block, `e2fsck` cleared the WAL outright, and the
  store came back as if freshly initialized (see Findings).
- **`syncfs` before arming a write fault** (`Device.settle`). Anything
  dirty at that point would otherwise be written by either the
  background flusher or the next commit, depending on timing. After
  the sync, the only writes that can hit the fault are the ones the
  next commit causes, and all of them fail the same way.

Read-fault gates are deterministic by construction: the target blocks
come from the file's extent map, a remount drops the page cache so
the reads reach the device, and the precondition probe proves the
fault is armed.

The power-cut recording is its own run, so its flush points come from
the run as it happened: journal commits and fsyncs interleave a little
differently each time, and the point count moved between 207 and 208
across our runs. The gate
checks every point of the recording it made, and each expectation
comes from that recording's marks, so the outcome does not depend on
the count.

## The gates

`pytests/faults/test_block_faults.py`. The workload
(`flow_driver.py`) sums N items per key with `stateful_map` and
appends every `(key, item, running total)` to a file outside the
faulty device. A gate in the source parks the execution at item 50
with the store durable exactly to it, so each test can kill the
process at a known resume point or arm a fault right before the next
commit.

| gate | fault | expected |
|---|---|---|
| `test_resume_over_unreadable_store[dust\|errseg\|nbdkit]` | every block of the store unreadable at resume | fails with a storage error, emits nothing; after healing, resumes at item 50 with no replay |
| `test_resume_over_unreadable_snapshot_pages[dust\|errseg]` | only the snapshot table's pages unreadable | same: the execution reads progress fine, then must fail rather than resume with part of its state |
| `test_write_error_at_boundary_commit` | every write fails right before an epoch commits | fails with a storage error; resumes from the last committed boundary, replaying only the failed batch |
| `test_storage_stall_fails_within_timeout` | storage server stopped right before a commit | fails within the nbd timeout instead of hanging; resumes after the volume is reattached |
| `test_dropped_writes_replay_from_last_real_boundary` | from item 50 on, writes acknowledged and dropped | the execution completes believing it durable; after a power cycle, resumes at item 50 with the right state and replays the rest |
| `test_corrupt_wal_frame_rolls_back` | a byte flipped in the last committed WAL frame | SQLite's frame checksum drops that transaction; resumes one boundary back |
| `test_corrupt_snapshot_is_detected` | a byte flipped in a snapshot in the main database file | **known gap, strict xfail**: resumes without error with a wrong total |
| `test_rolled_back_store_is_detected` | the WAL, holding all progress, lost while the store is closed | **known gap, strict xfail**: the store looks freshly initialized and the next execution starts over from the first item |
| `test_power_cut_at_every_flush_point` | a power cut at every FLUSH or FUA point of a full run, recorded by dm-log-writes; each point replayed onto a zeroed image | every point that mounts is clean after its journal replay; once the store exists, every point resumes from the last durable boundary or the batch after it, loses no item, and gets every running total right |

Each test checks its own precondition before it acts: an armed read
fault must make an O_DIRECT read of a target block fail, and a
corrupted byte must occur exactly once in the store and decode to the
flipped value. A test whose fault never fired would otherwise pass
while proving nothing. The xfail only accepts its own exception type,
so a broken precondition there fails too.

## Power cuts

`test_power_cut_at_every_flush_point` injects no fault. It records a
whole run instead. `env.sh record-up` puts dm-log-writes between the
nbd device and ext4, with its log on a loop device (`log.img`), so
mkfs and the store's init are recorded too. dm-log-writes logs each
write in the order it became durable: a write waits for the next
flush, which carries it into the log, and a FUA write is logged at
once. So the prefix of the log that ends at a FLUSH or FUA entry is a
state a power cut at that moment could leave.

`log_writes.py` replays each such prefix onto a zeroed image. It reads
the format of `drivers/md/dm-log-writes.c` (v6.8, the VM's kernel), with
no xfstests dependency. The image is mounted, so ext4 replays its
journal, and the workload resumes on it. The workload writes two kinds
of mark into the log (`flow_driver.py --marks`), which give each point
its expectation without consulting the store: `b<item>` when the source
is asked for the batch starting at `item`, which is after that
boundary's commit, and `o<item>` once that item's output line is on
disk.

Before `init_db_dir` returns (the `init` mark), the device is not a
store yet. A point there may fail to mount, or the store may refuse to
resume, as long as nothing is emitted. A point that does mount must
still pass `e2fsck -fn` after its journal replay. From then on, a
resume must start at or after the last boundary durable at the point
and at most one batch past it. So a silent fresh start, and a replay
from before the boundary, both fail. No item may be lost, and only
items at or after that boundary may be emitted twice.

Every flush point is replayed, up to 300. Beyond that, the gate samples
at a fixed stride, always keeping the first and last point and every
point after the last boundary. Before any point is checked, the log
must replay exactly to the device it recorded.

## Findings

- **Silent snapshot corruption.** SQLite checksums WAL frames but not
  the pages of the main database file, and a snapshot is a raw pickle
  in a BLOB. A bit flipped there, by anything the drive's ECC does not
  catch, is read back without error and restored as state. A
  per-snapshot checksum written with the snapshot would turn this into
  a deterministic failure.
- **Silent fresh start.** Until its first checkpoint, all of a store's
  progress lives in the WAL. If the storage loses the WAL (here: a
  write error on its metadata on a filesystem without a journal, then
  `e2fsck`), the store holds exactly what `init_db_dir` wrote, which no
  local check can tell apart from a store that never ran. The next
  execution starts over without an error. With a source that replays
  from its beginning, that duplicates history; with a Kafka source
  starting at the end of a topic, it silently skips what was not yet
  processed and drops all accumulated state. Only a witness outside the
  store can detect this: `KafkaEosSink`'s tail check does, since the
  topic is then ahead of the store.
- **"database disk image is malformed" can mean a bad sector.** Recent
  SQLite versions (3.40 here) map EIO on a read to
  `SQLITE_IOERR_CORRUPTFS`, which reaches the application as
  `SQLITE_CORRUPT`. An operator who sees
  this message should check the kernel log for I/O errors before
  deciding the store itself is damaged.
- **A power cut during init leaves a store that is refused, not
  resumed.** Until `init_db_dir` returns, the power-cut gate finds
  the directory missing or without its partitions, and the next
  execution fails with `NoPartitionsError` or `FileNotFoundError`
  before it emits anything. That is not silent, but the store is
  unusable until init runs again; the gate does not check re-running
  init on a half-initialized store.
- **A hung volume blocks the execution for as long as the storage
  lets it.** bytewax has no I/O watchdog of its own; the bound here
  comes from the nbd request timeout. Production volumes need their
  own timeouts, or a liveness probe that restarts the pod.

## Deployment requirements this implies

- A journaled filesystem (ext4 with its default `data=ordered`, or
  XFS). Not ext2, and not ext4 created without a journal.
- Write barriers on (never `nobarrier`), and storage that honors cache
  flushes; a drive that acknowledges and drops writes costs replay
  (the dropped-writes gate), and worse if it reorders them.
- `errors=remount-ro` (or `panic`) rather than `errors=continue`, so a
  failing disk stops writes instead of letting them continue past it.
- A liveness probe or storage timeout, since a hung volume blocks the
  execution for as long as the storage allows.

## Running

### Linux host

Needs Docker and these kernel modules on the host (containers cannot
load modules):

| module | gates | notes |
|---|---|---|
| `nbd` | all | `modprobe nbd nbds_max=16` |
| `dm_mod` | all | usually built in |
| `dm_flakey` | dropped writes | Ubuntu ships it in `linux-modules-extra-$(uname -r)` |
| `dm_log_writes` | power cut | Ubuntu ships it in `linux-modules-$(uname -r)` |
| `dm_dust` | bad blocks | Ubuntu kernels are built without it; `sudo ./build-dm-dust.sh` builds it for the running kernel from the matching upstream source (needs `linux-headers-$(uname -r)` and a compiler; the module is unsigned, so not under enforced Secure Boot) |

```
./scripts/blockfault/run.sh            # extra arguments go to pytest
BLOCKFAULT_REQUIRE=dust,flakey ./scripts/blockfault/run.sh
```

A gate whose optional target is missing is skipped with the reason,
unless `BLOCKFAULT_REQUIRE` names the target; then it fails.

### macOS (Docker Desktop)

Docker Desktop's LinuxKit kernel has nbd, device-mapper with the
`linear` and `error` targets, but neither dm-dust, dm-flakey nor
dm-log-writes, so `run.sh` there runs every gate except the `dust`
variants, the dropped-writes gate and the power-cut gate, which skip
with the reason. For those, run it in a Linux VM; an Ubuntu cloud
image under QEMU with HVF works, with `build-dm-dust.sh`.

### CI

`.github/workflows/block-faults.yml` installs the modules on the
hosted runner, builds dm-dust, and runs every gate with
`BLOCKFAULT_REQUIRE=dust,flakey`. `act` on macOS cannot run it, for
the same kernel reasons as Docker Desktop. The power-cut gate runs
there if `dm_log_writes` loads, and skips otherwise: `log-writes` is
not in `BLOCKFAULT_REQUIRE` yet, so the hosted runner's kernel has not
been checked for it.

## Exploring by hand

Inside the container (or as root on a Linux host):

```
env.sh up demo                # prints NBD=, DEV=, MNT=
env.sh blocks demo $MNT/f     # physical 4 KiB blocks of a file
env.sh dust demo 1088 1089    # those blocks now fail reads
env.sh probe demo 1088        # eio
env.sh heal demo
env.sh down demo
```

## Extending

The same stack hosts more device-mapper targets with no new
infrastructure: `dm-delay` for slow storage.
