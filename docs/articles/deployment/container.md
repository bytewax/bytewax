It is very simple to run a Bytewax dataflow program in a container. In this section we will describe how to do that.

## Bytewax Images in Docker Hub

Bytewax has several public container images available. Releases are available in Docker Hub with these python versions: 3.8, 3.9, 3.10 and 3.11.

We implement the following naming convention:

>**bytewax/bytewax:`BYTEWAX_VERSION`-python`PYTHON_VERSION`**

Following this convention, Bytewax `0.18.0` would have the images:
```
bytewax/bytewax:0.18.0-python3.8
bytewax/bytewax:0.18.0-python3.9
bytewax/bytewax:0.18.0-python3.10
bytewax/bytewax:0.18.0-python3.11
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
- A volume mapped to a directory which includes your python script file.
- A correspondent value for `BYTEWAX_PYTHON_FILE_PATH` environment variable.

To try this, first create an empty directory:

```bash
mkdir dataflows
```

Then create a file `dataflows/my_flow.py` with the following simple dataflow:
```python
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource

flow = Dataflow("test")
inp = op.input("in", flow, TestingSource(list(range(5))))
op.output(inp, "out", StdOutSink())
```

Now you can run it with:

```bash
docker run --rm --name=my-dataflow \
    -v ./dataflows:/bytewax/dataflows \
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

## How The Bytewax Image works

Bytewax images are structured in this way:
- A specifc version of Python and Bytewax is installed and managed in a virtual environment.
- Run an `entrypoint.sh` bash script which:
    - Sets the current directory in `BYTEWAX_WORKDIR` (default `/bytewax`).
    - Executes the `BYTEWAX_PYTHON_FILE_PATH` python script (there isn't a default value, you must set that environment variable always).
    - If the `BYTEWAX_KEEP_CONTAINER_ALIVE` environment variable is set to `true` executes an infinite loop to keep the container process running.

**Entrypoint.sh script**
```
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

Sometimes it is useful to explore the files and the environment configuration of a running container.

We are going to use the `BYTEWAX_KEEP_CONTAINER_ALIVE` environment variable to keep the container alive after the dataflow program has finished execution.

```bash
docker run --rm --name=my-dataflow \
    -v ./dataflows:/bytewax/dataflows \
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

```bash
docker exec -it my-dataflow /bin/sh
```

And you can explore the mounted volume, env var values or even run your dataflow again.

```
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

## Including Custom Dependencies in an Image

Bytewax's image includes a small number of modules: `bytewax` itself, `jsonpickle`, `pip`, `setuptools` and `wheel`

So if you try to run a script like the [wikistream](https://github.com/bytewax/bytewax/blob/main/examples/wikistream.py) example which requires additional modules:

```bash
# Fetch the dataflow first
wget https://raw.githubusercontent.com/bytewax/bytewax/v0.18.0/examples/wikistream.py -o dataflows/wikistream.py
# Then run it
docker run --rm --name=my-dataflow \
    -v ./dataflows:/bytewax/dataflows \
    -e BYTEWAX_PYTHON_FILE_PATH=dataflows.wikistream \
    bytewax/bytewax
```

You will get a `ModuleNotFoundError`:
```
Traceback (most recent call last):
  ...
  File "/bytewax/dataflows/wikistream.py", line 9, in <module>
    from aiohttp_sse_client.client import EventSource
ModuleNotFoundError: No module named 'aiohttp_sse_client'
Process ended.
```

You need to create your own container image and install the missing dependencies.
Create a file named `Dockerfile.custom` with the content below:

```
FROM bytewax/bytewax

RUN /venv/bin/pip install aiohttp-sse-client
```

Build the image:

```bash
docker build -f Dockerfile.custom -t bytewax-wikistream .
```

Now you can run the example using the new image:

```bash
docker run --rm --name=my-dataflow \
    -v ./dataflows:/bytewax/dataflows \
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

In summary, to create your own docker image, you will need to use the Bytewax images available in Docker Hub as your base image and then install the needed modules in the virtual environment that the image already has.

## Bytewax Container Images and Security

Our Images are based on `python:$PYTHON_VERSION-slim-bullseye` images which have a small attack surface (less than 50MB) and a very good scan report with zero CVE at the time of this writing.
