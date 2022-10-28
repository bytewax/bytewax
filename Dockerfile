# Builder to get maturin
FROM konstin2/maturin:v0.12.6 as maturin-builder

# Builder to get the package
FROM rust:1.61-slim-bullseye AS build

COPY --from=maturin-builder /usr/bin/maturin /usr/bin/maturin

ARG BYTEWAX_VERSION

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

COPY . /bytewax
WORKDIR /bytewax

RUN maturin build --interpreter python3.9
RUN /venv/bin/pip3 install /bytewax/target/wheels/bytewax-$BYTEWAX_VERSION-cp39-cp39-manylinux_2_31_x86_64.whl

# Final image
FROM gcr.io/distroless/python3-debian11:debug
COPY --from=build /venv /venv
WORKDIR /bytewax
COPY ./entrypoint.sh .

ENV BYTEWAX_WORKDIR=/bytewax

ENTRYPOINT ["/bin/sh", "-c", "./entrypoint.sh"]
