(xref-container)=
# Containers

In this chapter we are going to build a Docker image where we can run
a dataflow, and present the prebuilt images we offer.

## Your Dataflow

Let's start with a simple dataflow.

Create a new file `dataflow.py` with the following content:

```{testcode}
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("test")
inp = op.input("in", flow, TestingSource(list(range(5))))
op.output("out", inp, StdOutSink())
```

Run it locally to check everything's working fine:
```console
$ python -m bytewax.run dataflow
0
1
2
3
4
```

## Docker Image

You can run the dataflow inside a Docker image. You'll need an image
with Python support, and can just install Bytewax and run the
dataflow.

Create a `Dockerfile` with the following content:

```{code-block} Dockerfile
:substitutions:

# Start from a debian slim with python support
FROM python:3.11-slim-bullseye
# Setup a workdir where we can put our dataflow
WORKDIR /bytewax
# Install bytewax and the dependencies you need here
RUN pip install bytewax==|version|
# Copy the dataflow in the workdir
COPY dataflow.py dataflow.py
# And run it.
# Set PYTHONUNBUFFERED to any value to make python flush stdout,
# or you risk not seeing any output from your python scripts.
ENV PYTHONUNBUFFERED 1
CMD ["python", "-m", "bytewax.run", "dataflow"]
```

Now you can build the image:

```console
$ docker build . -t bytewax-custom
```

And check that everything's working:

```console
$ docker run --rm bytewax-custom
```

## Example: Docker Compose, Kafka Connector and Redpanda

Modify the Dockerfile to install optional `kafka` dependencies in
Bytewax:

```{code-block} diff
:substitutions:

- RUN pip install bytewax==|version|
+ RUN pip install bytewax[kafka]==|version|
```

Rebuild the image with:

```console
$ docker build . -t bytewax-custom
```

Modify the dataflow to read data from a kafka topic rather than the
testing input:

```{testcode}
from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

flow = Dataflow("test")
inp = kop.input("in", flow, brokers=["redpanda:9092"], topics=["in_topic"])
op.output("out", inp.oks, StdOutSink())
```

Now we can create a docker-compose file that runs both a Redpanda
instance and our dataflow. This is a simplified version of the docker
compose file offered in redpanda's docs for a development setup.

Create a file named `docker-compose.yml` with the following content:

```yaml
version: "3.7"
name: bytewax-redpanda
volumes:
  redpanda: null
services:
  redpanda:
    command:
      - redpanda start
      - --kafka-addr internal://0.0.0.0:9092
      - --advertise-kafka-addr internal://redpanda:9092
      - --pandaproxy-addr internal://0.0.0.0:8082
      - --advertise-pandaproxy-addr internal://redpanda:8082
      - --schema-registry-addr internal://0.0.0.0:8081
      - --rpc-addr redpanda:33145
      - --advertise-rpc-addr redpanda:33145
      - --smp 1
      - --memory 1G
      - --mode dev-container
      - --default-log-level=warn
    image: docker.redpanda.com/redpandadata/redpanda:v23.2.19
    container_name: redpanda
    volumes:
      - redpanda:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy:.+true' || exit 1"]
      interval: 15s
      timeout: 3s
      retries: 5
      start_period: 5s
  dataflow:
    image: bytewax-custom
    container_name: bytewax
    depends_on:
      redpanda:
        condition: service_healthy
```

Run it with:

```console
$ docker compose up
```

And you will see the output from the dataflow as soon as you start
producing messages in the topic. To produce messages with this setup,
you can use the `rpk` tool included in the Redpanda docker images:

```console
$ docker exec -it redpanda rpk topic produce in_topic
```

Write a message and press "Enter", then check the output from the
dataflow.

## Bytewax Images in Docker Hub

We showed how to build a custom image to run a Bytewax dataflow, but
Bytewax also offers some premade images, that are optimizied for build
size and have customizations options so that you don't always have to
create your own image from scratch. Releases are available in Docker
Hub with these python versions: 3.8, 3.9, 3.10 and 3.11.

We implement the following naming convention:

```bash
bytewax/bytewax:${BYTEWAX_VERSION}-python${PYTHON_VERSION}
```

Following this convention, Bytewax {{ version }} would have the
images:

```{code-block}
:substitutions:

bytewax/bytewax:|version|-python3.8
bytewax/bytewax:|version|-python3.9
bytewax/bytewax:|version|-python3.10
bytewax/bytewax:|version|-python3.11
```

And for the latest version of Bytewax:

```
bytewax/bytewax:latest-python3.8
bytewax/bytewax:latest-python3.9
bytewax/bytewax:latest-python3.10
bytewax/bytewax:latest-python3.11
```

The standard `latest` tag is equivalent to `latest-python3.9`.

## Using Bytewax Container Image locally

To run a dataflow program in a container you will need to set two things:

- A volume mapped to a directory which includes your python script
  file.

- A correspondent value for `BYTEWAX_PYTHON_FILE_PATH` environment
  variable.

To try this, first create an empty directory:

```console
$ mkdir dataflows
```

Then create a file `dataflows/my_flow.py` with the following simple
dataflow:

```{testcode}
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("test")
inp = op.input("in", flow, TestingSource(list(range(5))))
op.output("out", inp, StdOutSink())
```

Now you can run it with:

```console
$ docker run --rm --name=my-dataflow \
    -v $(pwd)/dataflows:/bytewax/dataflows \
    -e BYTEWAX_PYTHON_FILE_PATH=dataflows.my_flow \
    bytewax/bytewax
```

And after the image is pulled, you'll see the output of the dataflow:

```
# ...docker output first
0
1
2
3
4
Process ended.
```

(xref-container-custom-deps)=
## Including Custom Dependencies in an Image

Bytewax's image includes a small number of modules: `bytewax` itself,
`jsonpickle`, `pip`, `setuptools` and `wheel`

So if you try to run a dataflow which requires additional modules, you
will get a `ModuleNotFoundError`:

```{code-block} console
:substitutions:

$ # Fetch an example dataflow that requires one external dependency
$ wget https://raw.githubusercontent.com/bytewax/bytewax/|version|/examples/wikistream.py -o dataflows/wikistream.py
$ # Then run it
$ docker run --rm --name=my-dataflow \
    -v $(pwd)/dataflows:/bytewax/dataflows \
    -e BYTEWAX_PYTHON_FILE_PATH=dataflows.wikistream \
    bytewax/bytewax
Traceback (most recent call last):
  ...
  File "/bytewax/dataflows/wikistream.py", line 9, in <module>
    from aiohttp_sse_client.client import EventSource
ModuleNotFoundError: No module named 'aiohttp_sse_client'
Process ended.
```

You can inherit the base bytewax image and just install the missing
dependencies in a `RUN` step:

```Dockerfile
# Dockerfile
FROM bytewax/bytewax

RUN /venv/bin/pip install aiohttp-sse-client
```

Build the image:

```console
$ docker build -t bytewax-wikistream .
```

Now you can run the example using the new image:

```console
$ docker run --rm --name=my-dataflow \
    -v $(pwd)/dataflows:/bytewax/dataflows \
    -e BYTEWAX_PYTHON_FILE_PATH=dataflows.wikistream \
    bytewax-wikistream
```

And get the expected output:

```
commons.wikimedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 8)
en.wikipedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 6)
hr.wikipedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 1)
it.wikipedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 1)
ja.wikipedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 1)
sr.wikipedia.org, (WindowMetadata(open_time: 2023-12-15 14:34:52 UTC, close_time: 2023-12-15 14:34:54 UTC), 1)
...
```

(xref-container-image)=
## How The Bytewax Image works

Bytewax images are structured in this way:

- A specifc version of Python and Bytewax is installed and managed in
  a virtual environment.

- Run an `entrypoint.sh` bash script which:

    - Sets the current directory in `BYTEWAX_WORKDIR` (default
      `/bytewax`).

    - Executes the `BYTEWAX_PYTHON_FILE_PATH` python script (there
      isn't a default value, you must set that environment variable).

    - If the `BYTEWAX_KEEP_CONTAINER_ALIVE` environment variable is
      set to `true` executes an infinite loop to keep the container
      process running.

**Entrypoint.sh script**

```bash
#!/bin/sh

cd $BYTEWAX_WORKDIR
. /venv/bin/activate
python -m bytewax.run $BYTEWAX_PYTHON_FILE_PATH

echo 'Process ended.'

if [ "$BYTEWAX_KEEP_CONTAINER_ALIVE" = true ]
then
    echo 'Keeping container alive...';
    while :; do sleep 1; done
fi
```

## Running a Container interactively for Debbuging

Sometimes it is useful to explore the files and the environment
configuration of a running container.

We are going to use the `BYTEWAX_KEEP_CONTAINER_ALIVE` environment
variable to keep the container alive after the dataflow program has
finished execution.

```console
$ docker run --rm --name=my-dataflow \
    -v $(pwd)/dataflows:/bytewax/dataflows \
    -e BYTEWAX_PYTHON_FILE_PATH=dataflows.my_flow \
    -e BYTEWAX_KEEP_CONTAINER_ALIVE=true \
    bytewax/bytewax
```

The output:

```
0
1
2
3
4
Process ended.
Keeping container alive...
```

Then, in another terminal, you can run:

```console
$ docker exec -it my-dataflow /bin/sh
```

And you can explore the mounted volume, env var values or even run
your dataflow again.

```console
# ls -la
total 16
drwxr-xr-x 1 root root 4096 Apr 12 14:25 .
drwxr-xr-x 1 root root 4096 Apr 12 14:25 ..
-rwxr-xr-x 1 root root  221 Apr  7 11:39 entrypoint.sh
drwxrwxr-x 5 1000 1000 4096 Apr 12 14:16 dataflows

# env | grep BYTEWAX
BYTEWAX_WORKDIR=/bytewax
BYTEWAX_KEEP_CONTAINER_ALIVE=true
BYTEWAX_PYTHON_FILE_PATH=examples.pagerank:flow

# /venv/bin/python -m bytewax.run dataflows.my_flow
0
1
2
3
4
```

## Bytewax Container Images and Security

Our Images are based on `python:$PYTHON_VERSION-slim-bullseye` images
which have a small attack surface (less than 50MB) and a very good
scan report with zero CVE at the time of this writing.
