import os
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.tracing import OtlpTracingConfig, setup_tracing

tracer = setup_tracing(
    tracing_config=OtlpTracingConfig(
        url=os.getenv("BYTEWAX_OTLP_URL", "grpc://127.0.0.1:4317"),
        service_name="Tracing-example",
    ),
    log_level="TRACE",
)


def inp():
    for i in range(50):
        time.sleep(0.5)
        yield i


def double(x):
    return x * 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow("tracing")
stream = op.input("inp", flow, TestingSource(inp()))
stream = op.map("double", stream, double)
stream = op.map("minus_one", stream, minus_one)
stream = op.map("stringy", stream, stringy)
op.output("out", stream, StdOutSink())
