#!/usr/bin/env bash
# Build and load dm-dust for the running kernel when the distribution
# does not ship it.
#
# Ubuntu kernels, including the ones on GitHub's hosted runners, are
# built with "# CONFIG_DM_DUST is not set". dm-dust is one
# self-contained source file, so it builds as an external module
# against the running kernel's headers. The source comes from the
# upstream tag matching the kernel's major.minor version; a distro
# kernel whose device-mapper API diverged from it fails to compile
# here, loudly.
#
# Needs root, make, a C compiler and linux-headers-$(uname -r). The
# module is unsigned, so it taints the kernel and will not load where
# Secure Boot enforces module signatures.

set -euo pipefail

if [ -d /sys/module/dm_dust ] || modprobe dm_dust 2>/dev/null; then
    echo "dm-dust: already available"
    exit 0
fi

release=$(uname -r)
version=$(echo "$release" | cut -d. -f1-2)
build=/lib/modules/$release/build
[ -d "$build" ] || {
    echo "build-dm-dust: no kernel headers in $build (install linux-headers-$release)" >&2
    exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL -o "$work/dm-dust.c" \
    "https://raw.githubusercontent.com/torvalds/linux/v$version/drivers/md/dm-dust.c"
echo "obj-m := dm-dust.o" >"$work/Makefile"
make -s -C "$build" M="$work" modules
insmod "$work/dm-dust.ko"
echo "dm-dust: built from v$version and loaded"
