FROM konstin2/maturin:v0.12.6 as maturin-builder

COPY . /bytewax
WORKDIR /bytewax

RUN rustup install 1.58.1
RUN rustup override set 1.58.1
RUN cargo --version
RUN rustc --version

RUN maturin build --interpreter python3.9

FROM debian:11-slim AS build

ARG BYTEWAX_VERSION

RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

COPY --from=maturin-builder /bytewax/target/wheels/bytewax-$BYTEWAX_VERSION-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl /bytewax/target/wheels/bytewax-$BYTEWAX_VERSION-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl
RUN /venv/bin/pip3 install /bytewax/target/wheels/bytewax-$BYTEWAX_VERSION-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl

FROM gcr.io/distroless/python3-debian11:debug
COPY --from=build /venv /venv
WORKDIR /bytewax
COPY ./entrypoint.sh .

ENV BYTEWAX_WORKDIR=/bytewax

ENTRYPOINT ["/bin/sh", "-c", "./entrypoint.sh"]