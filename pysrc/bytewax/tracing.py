"""
Tracing and logging in bytewax are handled in the rust side,
to offer a really detailed view of what is happening in your dataflow.

By default, bytewax sends all "error" logs to the standard output.
This can be configured with an environment variable, "BYTEWAX_LOG".

See tracing-subscriber's documentation for possible values of the variable:
    https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html

For example, to set bytewax's logs to the "debug" level, and all other packages
to the "error" level, you can run the dataflow like this:

    $ BYTEWAX_LOG="bytewax=debug,error" python dataflow.py

But there's more. All the logs emitted by bytewax are structured,
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
    TracingConfig,
    JaegerConfig,
    OtlpTracingConfig,
    setup_tracing,
)
