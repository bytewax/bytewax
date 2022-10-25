import os

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.tracing import OltpTracingConfig

# from opentelemetry import trace
# from opentelemetry.sdk.resources import SERVICE_NAME, Resource
# from opentelemetry.sdk.trace import TracerProvider
# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
# from opentelemetry.sdk.trace.export import (BatchSpanProcessor, ConsoleSpanExporter)
#
# resource = Resource(attributes={SERVICE_NAME: "Tracing-example-2"})
# provider = TracerProvider(resource=resource)
# processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
# provider.add_span_processor(processor)
#
# # Sets the global default tracer provider
# trace.set_tracer_provider(provider)
#
# # Creates a tracer from the global tracer provider
# tracer = trace.get_tracer(__name__)
#
# span = tracer.start_as_current_span("Dataflow")


def input_builder(worker_index, worker_count, state):
    import time
    # Ignore state recovery here
    state = None
    for i in range(50):
        time.sleep(0.5)
        yield state, i


def double(x):
    # with tracer.start_span("double"):
    result = x * 2
    return result


def minus_one(x):
    # with tracer.start_span("minus-one"):
    return x - 1


def stringy(x):
    # with tracer.start_span("stringy"):
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
