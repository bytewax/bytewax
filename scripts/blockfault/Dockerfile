# Test image for the block-device fault gates. The bytewax source is
# mounted at /src and built inside the container; see run.sh.
FROM rust:1.86-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        nbdkit nbd-client dmsetup e2fsprogs util-linux \
        python3 python3-venv python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv \
    && /venv/bin/pip install --no-cache-dir maturin==1.5.1 pytest

ENV PATH=/venv/bin:$PATH
