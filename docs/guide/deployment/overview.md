(xref-deployment)=
# Deployment Overview

There are many ways you can run Bytewax, in this section we will highlight the main ways we recommend deploying Bytewax.

## The Bytewax Platform

We built the Bytewax Platform to make deploying, managing and operating dataflows easier. You can find out more about the platform features on the [Bytewax website](https://www.bytewax.io/platform) and the [platform documentation](https://platform.bytewax.io/).

## Container Based Deployment

Docker containers provide a good way to wrap up dependencies and run a containerized version of Bytewax across many different services and orchestration tools. From docker compose to Kubernetes. The [container documentation](#xref-container) provides an overview of how to leverage Bytewax in containers.

## Running Locally with waxctl

We recommend using `waxctl` locally to test running your dataflow with multiple processes, you can find more about `waxctl` on the [website](https://bytewax.io/waxctl).

Test it out locally after installing:

```console
$ waxctl run mydataflow.py -p 2
```

You can find more information in <project:#ref-waxctl>.

## Running in Kubernetes

Kubernetes is the most common orchestration platform for running containers. The recommended path to using Bytewax with Kubernetes is to use the [platform](https://platform.bytewax.io/) but you can use the publicly available [helm chart](#xref-helm) to run Bytewax on a Kubernetes cluster.

## Running on Cloud Instances

You can deploy a dataflow directly to a cloud instance on [AWS](#xref-waxctlaws) and [GCP](#xref-waxctlgcp) with `waxctl` doing the heavy lifting of setting up the machine and running the dataflow.
