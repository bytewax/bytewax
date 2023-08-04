# Notes about Bytewax CI

## Setup for Colab Support

To provide [Google Colab](https://colab.research.google.com/) support we need to build wheels compatible with GLIBC 2.27.
For that, we use a [custom docker image](Dockerfile.Colab) in the `linux_glibc_227_colab` job of our CI:

```yaml
...
  linux_glibc_227_colab:
    runs-on: ubuntu-latest
    container: bytewax/glib-2.27-builder:v0
    steps:
...
```

The steps made to build and push that custom image were as follows:

```sh
$ docker build -t bytewax/glib-2.27-builder:v1 -f Dockerfile.Colab .
$ docker login -u $BYTEWAX_DOCKERHUB_USER -p $BYTEWAX_DOCKERHUB_PASSWORD
$ docker push bytewax/glib-2.27-builder:v1
```
