import os
import time

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.tracing import OltpTracingConfig


def input_builder(worker_index, worker_count, state):
    state = None
    for i in range(50):
        time.sleep(0.5)
        yield state, i


def double(x):
    return x * 2


def minus_one(x):
    return x - 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(double)
flow.map(minus_one)
flow.map(stringy)
flow.capture(StdOutputConfig())


if __name__ == "__main__":
    spawn_cluster(
        flow,
        tracing_config=OltpTracingConfig(
            url=os.getenv("BYTEWAX_OTLP_URL", "grpc://127.0.0.1:4317"),
            service_name="Tracing-example",
        ),
        **parse.cluster_args(),
    )
