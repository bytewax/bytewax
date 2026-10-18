#!/usr/bin/env bash
# Run the block-device fault gates in a privileged container.
#
# The container builds bytewax from this checkout, puts a recovery
# store on the fault-injectable device that env.sh builds, and runs
# pytests/faults/. Kernel modules cannot be loaded from inside a
# container, so they must already be loaded on the host kernel:
#
#   nbd      required
#   dm_mod   required (usually built in)
#   dm_dust    latent bad-block gates; Ubuntu does not ship it, see
#              build-dm-dust.sh
#   dm_flakey  dropped-writes gate
#
# Gates whose optional target is missing are skipped, unless
# BLOCKFAULT_REQUIRE lists it (comma-separated), as CI does.
#
# On a Linux host this script tries `modprobe` itself (via sudo when
# not root). Extra pytest arguments are passed through.

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
image=${BLOCKFAULT_IMAGE:-bytewax-blockfault:dev}
cache=${BLOCKFAULT_CARGO_VOLUME:-bytewax-blockfault-cargo}

if [ "$(uname -s)" = Linux ]; then
    sudo=""
    [ "$(id -u)" = 0 ] || sudo="sudo -n"
    load() {
        [ -d "/sys/module/$1" ] || $sudo modprobe "$@" 2>/dev/null || true
    }
    load nbd nbds_max=16
    load dm_mod
    load dm_dust
    load dm_flakey
    [ -d /sys/module/nbd ] || {
        echo "run.sh: the host kernel has no nbd module loaded (modprobe nbd nbds_max=16)" >&2
        exit 1
    }
fi

docker build -q -t "$image" "$here" >/dev/null

docker run --rm --privileged \
    -v "$repo:/src" \
    -v "$cache:/cargo" \
    -e CARGO_HOME=/cargo/home \
    -e CARGO_TARGET_DIR=/cargo/target \
    -e BYTEWAX_BLOCKFAULT=1 \
    -e BLOCKFAULT_REQUIRE="${BLOCKFAULT_REQUIRE:-}" \
    -w /src \
    "$image" \
    bash -c '
        set -euo pipefail
        maturin build --release -q -o /tmp/wheels >/dev/null
        pip install -q /tmp/wheels/bytewax-*.whl
        # Import the installed wheel, not the source tree.
        cd /tmp
        exec pytest -p no:cacheprovider -v /src/pytests/faults "$@"
    ' bash "$@"
