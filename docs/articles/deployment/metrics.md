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
make bytewax talk to the opentelemetry collector integrated in a jaeger instance. We will use the
[wikistream.py](https://github.com/bytewax/bytewax/blob/main/examples/wikistream.py)
example as a reference, since it doesn't require any other setup. You
will need [docker](https://www.docker.com/) and
[docker-compose](https://docs.docker.com/compose/) to run this
example.

Create a folder where you'll keep the dataflow and two more files
we'll need to run everything.

```shell
mkdir bytewax-tracing
cd bytewax-tracing
```

Then create a docker compose file to run jaeger with the opentelemetry collector:

```yaml
# file: docker-compose.yml
version: "3"
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    ports:
      - "16686:16686"
      - "4317:4317"
      - "4318:4318"
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
source .venv/bin/activate # Or activate.fish on fish shell
pip install bytewax sseclient-py urllib3 aiohttp_sse_client
```

Now you can run it with:

```shell
python -m bytewax.run dataflow
```

Open your browser at [http://127.0.0.1:16686](http://127.0.0.1:16686)
and take a look at traces coming into Jaeger's UI.
