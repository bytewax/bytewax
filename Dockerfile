FROM konstin2/maturin:v0.12.5 as maturin-builder

COPY . /tiny-dancer
WORKDIR /tiny-dancer

RUN maturin build --interpreter python3.9

FROM debian:11-slim AS build
RUN apt-get update && \
    apt-get install --no-install-suggests --no-install-recommends --yes python3-venv gcc libpython3-dev && \
    python3 -m venv /venv && \
    /venv/bin/pip install --upgrade pip setuptools wheel

COPY --from=maturin-builder /tiny-dancer/target/wheels/tiny_dancer-0.1.0-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl /tiny-dancer/target/wheels/tiny_dancer-0.1.0-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl
RUN /venv/bin/pip3 install /tiny-dancer/target/wheels/tiny_dancer-0.1.0-cp39-cp39-manylinux_2_12_x86_64.manylinux2010_x86_64.whl

FROM gcr.io/distroless/python3-debian11:debug
COPY --from=build /venv /venv
WORKDIR /tiny-dancer
COPY ./pyexamples/ ./pyexamples/.
COPY ./entrypoint.sh .
