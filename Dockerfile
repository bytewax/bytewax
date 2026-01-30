# ----- Build stage: compile bytewax for linux/amd64 -----
FROM --platform=linux/amd64 python:3.14-slim AS build

ARG BYTEWAX_VERSION=0.22.0

# build deps
RUN apt-get update && apt-get install -y \
    build-essential curl pkg-config libssl-dev libsqlite3-dev \
 && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# maturin for building the wheel
RUN pip install --no-cache-dir maturin

WORKDIR /opt/bytewax
COPY . .

# build wheel for cp314 / linux x86_64
RUN maturin build --release --interpreter python3.14

# collect wheel in a clean place
RUN mkdir -p /dist && cp target/wheels/bytewax-${BYTEWAX_VERSION}-cp314-*.whl /dist/

# ----- Artifact stage: ONLY the wheel -----
FROM scratch AS artifact
COPY --from=build /dist /dist

