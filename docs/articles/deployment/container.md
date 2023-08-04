It is very simple to run a Bytewax dataflow program in a container. In this section we will describe how to do that.

## Bytewax Images in Docker Hub

Bytewax has several public container images available. Every library release is available in Docker Hub with these python versions: 3.7, 3.8, 3.9, 3.10 and 3.11.

We implement the following naming convention:

>**bytewax/bytewax:`BYTEWAX_VERSION`-python`PYTHON_VERSION`**

Following this convention, Bytewax `0.16.0` would have the images:
```
bytewax/bytewax:0.16.0-python3.7
bytewax/bytewax:0.16.0-python3.8
bytewax/bytewax:0.16.0-python3.9
bytewax/bytewax:0.16.0-python3.10
bytewax/bytewax:0.16.0-python3.11
```

And for the latest version of Bytewax:
```
bytewax/bytewax:latest-python3.7
bytewax/bytewax:latest-python3.8
bytewax/bytewax:latest-python3.9
bytewax/bytewax:latest-python3.10
bytewax/bytewax:latest-python3.11
```

The standard `latest` tag is equivalent to `latest-python3.9`.

## Using Bytewax Container Image locally

To run a dataflow program in a container you will need to set two things:
- A volume mapped to a directory which includes your python script file.
- A correspondent value for `BYTEWAX_PYTHON_FILE` environment variable.

For example:

```bash
docker run --rm --name=my-dataflow \
    -v /var/bytewax/examples:/bytewax/examples \
    -e BYTEWAX_PYTHON_FILE=examples.pagerank:flow \
    bytewax/bytewax
```
The output should be something like this:
```
Unable to find image 'bytewax/bytewax:latest' locally
latest: Pulling from bytewax/bytewax
c229119241af: Already exists
5a3ae98ea812: Already exists
fb6cb411715f: Pull complete
f83c4a78fcb9: Pull complete
4835b7464fe4: Pull complete
dad0b2a57b01: Pull complete
9666deee65de: Pull complete
d15d6c35474d: Pull complete
Digest: sha256:c6c213c3753b8a4ffa988f8a8d9e36669357c6970d2d2e74c7d559c98ef63589
Status: Downloaded newer image for bytewax/bytewax:latest
('5', 1.8783333333333332)
('4', 0.5325)
('2', 1.028333333333333)
('1', 2.0625)
('6', 0.3625)
('7', 0.32)
('3', 0.8158333333333333)
Process ended.
```

### A comment about `docker run` and volumes

It's necessary to use an absolute local path to map your local directory with the running container directory. So you can't use paths like `./my-directory` or `~/another-directory` in the left side of the `-v` flag value.

In our example we used `-v /var/bytewax/examples:/bytewax/examples`, so our local path was `/var/bytewax/examples`.

## How The Bytewax Image works

Bytewax images are structured in this way:
- A specifc version of Python and Bytewax is installed and managed in a virtual environment.
- Run an `entrypoint.sh` bash script which:
    - Sets the current directory in `BYTEWAX_WORKDIR` (default `\bytewax`).
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
    -v /var/bytewax/examples:/bytewax/examples \
    -e BYTEWAX_PYTHON_FILE_PATH=examples.pagerank:flow \
    -e BYTEWAX_KEEP_CONTAINER_ALIVE=true \
    bytewax/bytewax
```

The output:
```
('1', 2.0625)
('5', 1.8783333333333332)
('2', 1.028333333333333)
('3', 0.8158333333333332)
('4', 0.5325)
('7', 0.32)
('6', 0.3625)
Process ended.
Keeping container alive...
```

Then, in another terminal, you could run:

```bash
docker exec -it my-dataflow /bin/sh
```

And then you could explore which files are in the mounted volume, see env var values or even run your dataflow again.

```
# ls -la
total 16
drwxr-xr-x 1 root root 4096 Apr 12 14:25 .
drwxr-xr-x 1 root root 4096 Apr 12 14:25 ..
-rwxr-xr-x 1 root root  221 Apr  7 11:39 entrypoint.sh
drwxrwxr-x 5 1000 1000 4096 Apr 12 14:16 examples

# env | grep BYTEWAX
BYTEWAX_WORKDIR=/bytewax
BYTEWAX_KEEP_CONTAINER_ALIVE=true
BYTEWAX_PYTHON_FILE_PATH=examples.pagerank:flow

# /venv/bin/python -m bytewax.run examples.pagerank:flow
('5', 1.8783333333333332)
('6', 0.3625)
('7', 0.32)
('4', 0.5325)
('3', 0.8158333333333332)
('1', 2.0625)
('2', 1.028333333333333)
```

## Including Custom Dependencies in an Image

Bytewax image includes a small number of modules installed:
```
Package      Version
------------ ---------
bytewax      0.16.0
pip          22.0.4
setuptools   62.0.0
wheel        0.37.1
```

So if you try to run a script like the [translator](https://github.com/bytewax/bytewax/tree/main/examples/translator.py) example which requires additional modules:

```bash
docker run --rm --name=my-dataflow \
    -v /var/bytewax/examples:/bytewax/examples \
    -e BYTEWAX_PYTHON_FILE_PATH=examples.translator:flow \
    bytewax/bytewax
```
You will get a `ModuleNotFoundError`:
```
Traceback (most recent call last):
  File "/bytewax/examples/translator.py", line 3, in <module>
    from transformers import pipeline
ModuleNotFoundError: No module named 'transformers'
Process ended.
```

So, in that case you will need to create your own container image.
To demonstrate this, we are going to create a file `Dockerfile.custom` with the content below:

```
FROM bytewax/bytewax

RUN /venv/bin/pip install transformers torch torchvision torchaudio
```
And create an image running this command:

```bash
docker build -f Dockerfile.custom -t bytewax-translator .
```

Now we can run the example using the new image:

```bash
docker run --rm --name=my-dataflow \
    -v /var/bytewax/examples:/bytewax/examples \
    -e BYTEWAX_PYTHON_FILE_PATH=examples.translator:flow \
    bytewax-translator
```

And we get this result:

```
No model was supplied, defaulted to t5-base (https://huggingface.co/t5-base)
Downloading: 100%|██████████| 1.17k/1.17k [00:00<00:00, 934kB/s]
Downloading: 100%|██████████| 850M/850M [00:11<00:00, 78.8MB/s]
Downloading: 100%|██████████| 773k/773k [00:00<00:00, 2.17MB/s]
Downloading: 100%|██████████| 1.32M/1.32M [00:00<00:00, 3.13MB/s]
Blue jean baby, L.A. lady, seamstress for the band -> Blue Jean Baby, L.A. Lady, Näherin für die Band
Pretty eyed, pirate smile, you'll marry a music man -> Hübsches Auge, Piratenlächeln, heiraten Sie einen Musikmann
Ballerina, you must have seen her dancing in the sand -> Ballerina, Sie müssen sie im Sand tanzen gesehen haben
And now she's in me, always with me, tiny dancer in my hand -> Und nun ist sie in mir, immer mit mir, winzige Tänzerin in meiner Hand
Process ended.
```

In summary, to create your own docker image, you will need to use the Bytewax images available in Docker Hub as your base image and then install the needed modules in the virtual environment that the image already has.

## Bytewax Container Images and Security

Our Images are based on `python:$PYTHON_VERSION-slim-bullseye` images which have a small attack surface (less than 50MB) and a very good scan report with zero CVE at the time of this writing.
