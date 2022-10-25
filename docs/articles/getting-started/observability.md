## Instrumentation

Bytewax is instrumented to offer observability of your dataflow.

The default configuration logs anything at the log level `ERROR` to standard output.  
You can control the log level with an environment variable, `BYTEWAX_LOG`.  
If you want to see all the messages bytewax emits, you can set it to "trace":

```
BYTEWAX_LOG="bytewax=trace" python dataflow.py
```

The `TRACE` level includes everything that would be sent to an opentelemetry compatible backend,
like [Jaeger](https://www.jaegertracing.io/), or the [Opentelemetry Collector](https://opentelemetry.io/docs/collector/).  
It is really verbose, and your stdoutput will be flooded with logs, so use it carefully.

## Try it
Let's try to see what `jaeger` can show us about a dataflow.
We will make bytewax talk to the opentelemetry collector, and let the collector send everything to a jaeger instance.
We will use the [wikistream.py](https://github.com/bytewax/bytewax/blob/main/examples/wikistream.py) example as a reference,
since it doesn't require any other setup.
You will need [docker](https://www.docker.com/) and [docker-compose](https://docs.docker.com/compose/) to run this example.

Create a folder where you'll keep the dataflow and two more files we'll need to run everything.  
```shell
mkdir bytewax-tracing
cd bytewax-tracing
```

Now create a configuration file for the collector, telling it to receive traces from grpc and send them to jaeger,
with the following content:
```yaml
# file: otel-collector-config.yml
receivers:
  otlp:
    protocols:
      grpc:

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: []
      exporters: [jaeger]
```

Then create a docker compose file to run both jaeger and the collector, mounting the config we just wrote:
```yaml
# file: docker-compose.yml
version: "3"
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"
      - "14250"

  otel-collector:
    image: otel/opentelemetry-collector:latest
    command: ["--config=/etc/otel-collector-config.yml"]
    volumes:
      - type: "bind"
        source: ./otel-collector-config.yml
        target: /etc/otel-collector-config.yml
    ports:
      - "4317:4317"
    depends_on:
      - jaeger
```

Now run `docker compose up` and everything should be up and running.

Now we need the dataflow. Download the example in this folder:
```shell
curl https://raw.githubusercontent.com/bytewax/bytewax/main/examples/wikistream.py -o dataflow.py
```

To instrument your dataflow, pass a tracing configuration to the `spawn_cluster` function:

```python
# file: dataflow.py
from bytewax.tracing import OltpTracingConfig
#
# ...rest of the file
#
if __name__ == "__main__":
    spawn_cluster(
        flow,
        recovery_config=SqliteRecoveryConfig("."),
        # Add this lines
        tracing_config=OltpTracingConfig(
            service_name="Wikistream",
            url="grpc://127.0.0.1:4317",
        )
    )
```

Create a virtual environment and install the needed dependencies:

```shell
python3 -m venv .venv
source .venv/bin/activate # Or activate.fish on fish shell
# TODO: just use `pip install bytewax` when released
pip install git+https://github.com/bytewax/bytewax.git@observability
pip install sseclient-py urllib3
```

Now you can run it with:
```shell
python dataflow.py
```

Open your browser at [http://127.0.0.1:16686](http://127.0.0.1:16686) and take a look at traces coming into Jaeger's UI.
