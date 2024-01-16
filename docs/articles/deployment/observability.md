Bytewax is instrumented to offer observability of your dataflow.

The default configuration logs anything at the log level `ERROR` to
standard output. You can control the log level by passing the
`log_level` parameter to the `setup_tracing` function. If you want to
see all the messages bytewax emits, set the level to `TRACE`.

The `TRACE` level includes everything that would be sent to an
opentelemetry compatible backend, like
[Jaeger](https://www.jaegertracing.io/), or the [Opentelemetry
Collector](https://opentelemetry.io/docs/collector/). It is really
verbose, and your stdoutput will be flooded with logs, so use it
carefully.

## Try it

Let's try to see what `jaeger` can show us about a dataflow. We will
make bytewax talk to the jaeger collector, and visualize metrics in the
jaeger instance. We will use the
[wikistream.py](https://github.com/bytewax/bytewax/blob/main/examples/wikistream.py)
example as a reference. You will need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) to run this example.

Create a folder where you'll keep the dataflow and two more files
we'll need to run everything.

```shell
mkdir bytewax-tracing
cd bytewax-tracing
```

Now create a docker compose file to run jaeger with the
collector included and exposed to the local network,
so that we can run our dataflow locally:

```yaml
# file: docker-compose.yml
version: "3"
services:
  jaeger:
    image: jaegertracing/all-in-one:1.52
    network_mode: "host"
    environment:
      - COLLECTOR_OTLP_ENABLED=true
```

Now run `docker compose up` and everything should be up and running.

Now we need the dataflow. Download the example in this folder:

```shell
curl https://raw.githubusercontent.com/bytewax/bytewax/main/examples/wikistream.py \
  -o dataflow.py
```

To instrument your dataflow, call `setup_tracing` from
`bytewax.tracing` with the config object you want, and keep the
returned object around (if you don't assign to the `tracer` variable,
tracing would not work)

```python
# file: dataflow.py
from bytewax.tracing import OtlpTracingConfig, setup_tracing

tracer = setup_tracing(
    tracing_config=OtlpTracingConfig(
        service_name="Wikistream",
        url="grpc://127.0.0.1:4317",
    ),
    log_level="ERROR",
)
#
# ...rest of the file
#
```

Create a virtual environment and install the needed dependencies:

```shell
python3 -m venv .venv
source .venv/bin/activate # Or `.venv/bin/activate.fish` on fish shell
pip install bytewax aiohttp-sse-client
```

Now you can run it with:

```shell
python -m bytewax.run dataflow
```

Open your browser at [http://127.0.0.1:16686](http://127.0.0.1:16686)
and take a look at traces coming into Jaeger's UI.

## Using waxctl

Using waxctl with your own kubernetes cluster, you can deploy a dataflow with access to a Jaeger
instance inside your cluster.

You'll need to create a `values.yml` file to enable Jaeger and the OTEL collector in bytewax's helm chart:

```yml
# values.yml
opentelemetry-collector:
  enabled: true
jaeger:
  enabled: true
  elasticsearch:
    replicas: 1
    minimumMasterNodes: 1
```

Then use a dataflow with a tracing configuration that takes Jaeger's url from an env var
that will be injected by the helm chart:

```python
# dataflow.py
import os
import time

from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.tracing import OtlpTracingConfig, setup_tracing

JAEGER_URL = os.getenv("BYTEWAX_OTLP_URL")
SERVICE_NAME = "tracing-example"

tracer = setup_tracing(
    tracing_config=OtlpTracingConfig(
        service_name=SERVICE_NAME,
        url=JAEGER_URL,
    ),
)


def inp():
    i = 0
    while True:
        time.sleep(0.5)
        yield i
        i += 1


def stringy(x):
    return f"<dance>{x}</dance>"


flow = Dataflow("tracing")
stream = op.input("inp", flow, TestingSource(inp()))
stream = op.map("stringy", stream, stringy)
op.output("out", stream, StdOutSink())
```

And finally deploy it with:

```bash
waxctl df deploy dataflow.py \
  --name tracing-example \
  -V ./values.yml
```

Now you can access your Jaeger instance by port-forwarding the web ui in the deployed jaeger service:

```bash
kubectl port-forward svc/tracing-example-jaeger-query 8080:80
```

And visit `http://localhost:8080` to go to Jaeger's UI.

If you want to deploy a new dataflow, and reuse the same Jaeger/collector instances, you'll need a different
`values.yml`. Since you don't want to provision new instances, but only point to the right one, you
just need to set the url:

```yml
# values.yml
# Replace `tracing-example-opentelemetry-collector` with the url of the previously
# provisioned open telemetry collector, this is the default one:
customOtlpUrl: http://tracing-example-opentelemetry-collector:4317
```

Refer to the helm chart's documentation [here](https://bytewax.github.io/helm-charts/) to know all
the possible configuration options.
