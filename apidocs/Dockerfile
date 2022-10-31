# Docker image only used to reduilb pdocs if you have any trouble
# setting up the environment (it happens on Windows mostly)
FROM rust:1.61-slim-bullseye AS build

# Install all the needed dependencies and create a virtualenv
RUN apt-get update && \
  apt-get install --no-install-suggests --no-install-recommends --yes \
        build-essential \
        cmake \
        gcc \
        libpython3-dev \
        libsasl2-dev \
        libssl-dev \
        make \
        openssl \
        patchelf \
        pkg-config \
        protobuf-compiler \
        python3-venv && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

# The build of bytewax is done in the entrypoint,
# so that you can use this image with your current code
COPY apidocs/entrypoint.sh /

ENTRYPOINT ["/bin/sh", "-c", "/entrypoint.sh"]
