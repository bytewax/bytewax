(xref-deployment)=
# Deployment Overview

There are many ways you can run Bytewax, in this section we will highlight the main ways we recommend deploying Bytewax.

## Container Based Deployment

Docker containers provide a good way to wrap up dependencies and run a containerized version of Bytewax across many different services and orchestration tools. From docker compose to Kubernetes. The [container documentation](#xref-container) provides an overview of how to leverage Bytewax in containers.

## Running Locally with waxctl

We recommend using `waxctl` locally to test running your dataflow with multiple processes, you can download `waxctl` from its [GitHub releases](https://github.com/bytewax/waxctl-downloads/releases).

Test it out locally after installing:

```console
$ waxctl run mydataflow.py -p 2
```

You can find more information in <project:#xref-waxctl>.

## Running in Kubernetes

Kubernetes is the most common orchestration platform for running containers. You can use the publicly available [helm chart](#xref-helm) to run Bytewax on a Kubernetes cluster.

## Running on Cloud Instances

You can deploy a dataflow directly to a cloud instance on [AWS](#xref-waxctlaws) and [GCP](#xref-waxctlgcp) with `waxctl` doing the heavy lifting of setting up the machine and running the dataflow.
