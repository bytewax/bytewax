"""
Tracing and logging in bytewax are handled in the rust side,
to offer a really detailed view of what is happening in your dataflow.

By default, bytewax sends all "error" logs to the standard output.
This can be configured with the `log_level` parameter of the
`setup_tracing` function.

All the logs emitted by bytewax are structured,
and can be used to setup proper tracing for the dataflow.
To do that you need to talk to a service that collects
and shows data coming from bytewax.

There two possibilities out of the box:
    - Jaeger
    - Opentelemetry collector

### [Openetelemetry Collector](https://opentelemetry.io/docs/collector/)

The Opentelemetry collector is the recommended choice, since it can talk
to a lot of different backends, jaeger included, and you can swap your
tracing infrastructure without touching the dataflow configuration,
since the dataflow only talks to the collector.

### [Jaeger](https://www.jaegertracing.io/)

Bytewax can send traces directly to jaeger, without going through
the opentelemetry collector.
This makes the setup easier, but it's less flexible.
"""
from .bytewax import (  # noqa: F401
    JaegerConfig,
    OtlpTracingConfig,
    setup_tracing,
    TracingConfig,
)

__all__ = [
    "TracingConfig",
    "JaegerConfig",
    "OtlpTracingConfig",
    "setup_tracing",
]
