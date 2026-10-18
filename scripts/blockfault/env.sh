#!/usr/bin/env bash
# Build and control a fault-injectable block device for the recovery
# store.
#
#   sparse image -> nbdkit (error filter) -> /dev/nbdN -> device-mapper
#   -> ext4 (journaled) -> mount point holding the recovery directory
#
# Every layer has a switch. nbdkit fails all reads or all writes while
# a trigger file exists, and SIGSTOP on nbdkit stalls the storage like
# a hung network volume. The device-mapper table can be swapped for
# dm-dust (latent bad blocks: reads fail, a write heals the block) or
# for `error` segments over chosen blocks (reads and writes fail), or
# for dm-flakey dropping every write it acknowledges.
#
# Run as root on Linux, or inside the privileged container that run.sh
# starts. Needs the nbd and dm-mod kernel modules; dm-dust and
# dm-flakey are optional.
# See README.md for the scenarios built on top of this.
#
# Usage: env.sh <command> <name> [args...]
#
#   up NAME [SIZE_MB] [journal|nojournal]   create and mount a device
#                                           (journaled ext4 unless told)
#   down NAME                               tear it down
#   remount NAME                            unmount and mount (cold cache)
#   umount NAME / mount NAME                unmount / mount only
#   fsck NAME                               unmount, e2fsck -fy, mount
#   restart NAME                            reattach the storage from scratch
#   blocks NAME FILE...                     physical 4 KiB blocks of files
#   probe NAME BLOCK                        O_DIRECT read: prints ok or eio
#   nbd-fail NAME read|write|both           nbdkit fails that I/O
#   stall NAME / unstall NAME               SIGSTOP / SIGCONT nbdkit
#   dust NAME BLOCK...                      dm-dust bad blocks, enabled
#   errseg NAME BLOCK...                    dm error segments over blocks
#   drop-writes NAME                        dm-flakey acks writes and drops them
#   heal NAME                               clear every fault
#   flip NAME OFFSET                        xor one byte on the device
#   targets                                 device-mapper targets available
#   env NAME                                print the environment

set -euo pipefail

# Containers have no udev: let dmsetup create device nodes itself.
export DM_DISABLE_UDEV=1

ROOT=${BLOCKFAULT_ROOT:-/run/blockfault}
BS=4096
SECTORS_PER_BLOCK=$((BS / 512))
NBD_TIMEOUT=${BLOCKFAULT_NBD_TIMEOUT:-5}

die() {
    echo "blockfault: $*" >&2
    exit 1
}

load() {
    DIR="$ROOT/$1"
    [ -f "$DIR/env" ] || die "no environment named $1 (run: env.sh up $1)"
    # shellcheck disable=SC1091
    . "$DIR/env"
}

save() {
    cat >"$DIR/env" <<EOF
NAME=$NAME
DIR=$DIR
NBD=$NBD
DM=$DM
DEV=/dev/mapper/$DM
MNT=$DIR/mnt
SECTORS=$SECTORS
EOF
}

# Device-mapper prints tables with major:minor numbers, not paths.
dm_devno() {
    echo "$(stat -c '%t' "$NBD" | xargs printf '%d'):$(stat -c '%T' "$NBD" | xargs printf '%d')"
}

has_target() {
    dmsetup targets | awk '{print $1}' | grep -qx "$1"
}

nbdkit_pid() {
    cat "$DIR/nbdkit.pid" 2>/dev/null || true
}

start_nbdkit() {
    rm -f "$DIR/nbd.sock" "$DIR/nbdkit.pid"
    nbdkit -U "$DIR/nbd.sock" -P "$DIR/nbdkit.pid" --filter=error \
        file "$DIR/disk.img" error=EIO \
        error-pread-rate=100% error-pread-file="$DIR/trigger/read" \
        error-pwrite-rate=100% error-pwrite-file="$DIR/trigger/write"
    for _ in $(seq 100); do
        [ -S "$DIR/nbd.sock" ] && return 0
        sleep 0.05
    done
    die "nbdkit did not open $DIR/nbd.sock"
}

free_nbd() {
    local n
    for n in /sys/block/nbd*; do
        [ -e "$n" ] || break
        if [ ! -e "$n/pid" ] && [ "$(cat "$n/size")" = 0 ]; then
            echo "/dev/$(basename "$n")"
            return 0
        fi
    done
    die "no free /dev/nbd* device (load the module: modprobe nbd nbds_max=16)"
}

attach() {
    NBD=$(free_nbd)
    # -N "": nbdkit serves one unnamed export. -t bounds how long a
    # request may wait on a stalled server before it fails with EIO.
    nbd-client -unix "$DIR/nbd.sock" "$NBD" -b "$BS" -t "$NBD_TIMEOUT" -N "" \
        >/dev/null 2>&1 || die "nbd-client could not attach $NBD"
    SECTORS=$(blockdev --getsz "$NBD")
    dmsetup create "$DM" --table "0 $SECTORS linear $NBD 0"
    dmsetup mknodes "$DM"
}

detach() {
    dmsetup resume "$DM" >/dev/null 2>&1 || true
    dmsetup remove --force "$DM" >/dev/null 2>&1 || true
    nbd-client -d "$NBD" >/dev/null 2>&1 || true
}

do_mount() {
    mountpoint -q "$MNT" || mount -o errors=continue "/dev/mapper/$DM" "$MNT"
}

do_umount() {
    if mountpoint -q "$MNT"; then
        sync -f "$MNT" 2>/dev/null || true
        umount "$MNT" 2>/dev/null || umount -l "$MNT"
    fi
}

reload() {
    printf '%s\n' "$@" >"$DIR/table"
    dmsetup reload "$DM" "$DIR/table"
    # No filesystem freeze: freezing syncs the filesystem, which fails
    # with any writeback error an earlier fault recorded.
    dmsetup suspend --nolockfs "$DM"
    dmsetup resume "$DM"
}

# Collapse block numbers into runs and emit a device-mapper table that
# maps the device linearly except for `error` segments over the runs.
errseg_table() {
    python3 - "$SECTORS" "$NBD" "$SECTORS_PER_BLOCK" "$@" <<'PY'
import sys

sectors, dev, spb = int(sys.argv[1]), sys.argv[2], int(sys.argv[3])
blocks = sorted({int(b) for b in sys.argv[4:]})
runs = []
for b in blocks:
    if runs and runs[-1][1] == b:
        runs[-1][1] = b + 1
    else:
        runs.append([b, b + 1])
pos = 0
for start, end in runs:
    s, e = start * spb, end * spb
    if s > pos:
        print(f"{pos} {s - pos} linear {dev} {pos}")
    print(f"{s} {e - s} error")
    pos = e
if pos < sectors:
    print(f"{pos} {sectors - pos} linear {dev} {pos}")
PY
}

cmd_up() {
    local size_mb=${2:-128} journal=${3:-journal}
    NAME=$1
    DIR="$ROOT/$NAME"
    DM="bwx-$NAME"
    [ ! -e "$DIR/env" ] || die "environment $NAME already exists"
    mkdir -p "$DIR/trigger" "$DIR/mnt"
    truncate -s "${size_mb}M" "$DIR/disk.img"
    start_nbdkit
    attach
    MNT="$DIR/mnt"
    local opts=(-q -F -b "$BS")
    # Journaled by default, like a real deployment: after a failed
    # write, journal replay brings the metadata back to its last
    # committed state. Without a journal, a write error that lands on
    # metadata (an extent-tree block, say) can leave e2fsck no choice
    # but to clear a file outright.
    [ "$journal" = journal ] || opts+=(-O ^has_journal)
    mkfs.ext4 "${opts[@]}" "/dev/mapper/$DM"
    do_mount
    save
    cat "$DIR/env"
}

cmd_down() {
    load "$1"
    rm -f "$DIR/trigger/"*
    local pid
    pid=$(nbdkit_pid)
    [ -z "$pid" ] || kill -CONT "$pid" 2>/dev/null || true
    do_umount 2>/dev/null || true
    detach
    [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
    rm -rf "$DIR"
}

cmd_restart() {
    load "$1"
    rm -f "$DIR/trigger/"*
    local pid
    pid=$(nbdkit_pid)
    if [ -n "$pid" ]; then
        kill -CONT "$pid" 2>/dev/null || true
        kill -KILL "$pid" 2>/dev/null || true
    fi
    umount -l "$MNT" 2>/dev/null || true
    detach
    start_nbdkit
    attach
    e2fsck -fy "/dev/mapper/$DM" >/dev/null 2>&1 || [ $? -lt 4 ] ||
        die "e2fsck could not repair /dev/mapper/$DM"
    do_mount
    save
}

cmd_fsck() {
    load "$1"
    do_umount
    e2fsck -fy "/dev/mapper/$DM" >/dev/null 2>&1 || [ $? -lt 4 ] ||
        die "e2fsck could not repair /dev/mapper/$DM"
    do_mount
}

cmd_blocks() {
    load "$1"
    shift
    local f
    for f in "$@"; do
        [ -f "$f" ] || die "no such file: $f"
        sync "$f"
        # Extent lines look like "0: 0.. 7: 1088.. 1095: 8: ...".
        filefrag -v -b"$BS" "$f" | awk '
            $1 ~ /^[0-9]+:$/ {
                line = $0
                gsub(/\.\./, " ", line)
                gsub(/:/, " ", line)
                split(line, f, " ")
                for (b = f[4]; b <= f[5]; b++) print b
            }'
    done | sort -n -u
}

cmd_probe() {
    load "$1"
    if dd if="/dev/mapper/$DM" of=/dev/null bs="$BS" skip="$2" count=1 \
        iflag=direct status=none 2>/dev/null; then
        echo ok
    else
        echo eio
    fi
}

cmd_nbd_fail() {
    load "$1"
    case "${2:-}" in
    read) touch "$DIR/trigger/read" ;;
    write) touch "$DIR/trigger/write" ;;
    both) touch "$DIR/trigger/read" "$DIR/trigger/write" ;;
    *) die "nbd-fail takes read, write or both" ;;
    esac
}

cmd_dust() {
    load "$1"
    shift
    has_target dust || die "dm-dust is not available (load it on the host: modprobe dm_dust)"
    reload "0 $SECTORS dust $NBD 0 $BS"
    local b
    for b in "$@"; do
        dmsetup message "$DM" 0 addbadblock "$b" >/dev/null
    done
    dmsetup message "$DM" 0 enable >/dev/null
}

cmd_errseg() {
    load "$1"
    shift
    [ $# -gt 0 ] || die "errseg needs at least one block"
    local table
    table=$(errseg_table "$@")
    mapfile -t lines <<<"$table"
    reload "${lines[@]}"
}

cmd_drop_writes() {
    load "$1"
    has_target flakey || die "dm-flakey is not available (load it on the host: modprobe dm_flakey)"
    # Up for 0 s and down for 1 s: always down, and while down every
    # write is acknowledged and discarded, like a disk that lies about
    # its cache.
    reload "0 $SECTORS flakey $NBD 0 0 1 1 drop_writes"
}

cmd_heal() {
    load "$1"
    rm -f "$DIR/trigger/"*
    local pid
    pid=$(nbdkit_pid)
    [ -z "$pid" ] || kill -CONT "$pid" 2>/dev/null || true
    if [ "$(dmsetup table "$DM")" != "0 $SECTORS linear $(dm_devno) 0" ]; then
        reload "0 $SECTORS linear $NBD 0"
    fi
}

cmd_flip() {
    load "$1"
    python3 - "/dev/mapper/$DM" "$2" <<'PY'
import os
import sys

dev, offset = sys.argv[1], int(sys.argv[2])
fd = os.open(dev, os.O_RDWR)
try:
    old = os.pread(fd, 1, offset)
    os.pwrite(fd, bytes([old[0] ^ 0x01]), offset)
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

main() {
    local cmd=${1:-}
    shift || true
    case "$cmd" in
    up) cmd_up "$@" ;;
    down) cmd_down "$@" ;;
    remount)
        load "$1"
        do_umount
        do_mount
        ;;
    umount)
        load "$1"
        do_umount
        ;;
    mount)
        load "$1"
        do_mount
        ;;
    fsck) cmd_fsck "$@" ;;
    restart) cmd_restart "$@" ;;
    blocks) cmd_blocks "$@" ;;
    probe) cmd_probe "$@" ;;
    nbd-fail) cmd_nbd_fail "$@" ;;
    stall)
        load "$1"
        kill -STOP "$(nbdkit_pid)"
        ;;
    unstall)
        load "$1"
        kill -CONT "$(nbdkit_pid)"
        ;;
    dust) cmd_dust "$@" ;;
    errseg) cmd_errseg "$@" ;;
    drop-writes) cmd_drop_writes "$@" ;;
    heal) cmd_heal "$@" ;;
    flip) cmd_flip "$@" ;;
    targets) dmsetup targets ;;
    env)
        load "$1"
        cat "$DIR/env"
        ;;
    *) die "unknown command: ${cmd:-<none>} (see the header of $0)" ;;
    esac
}

main "$@"
