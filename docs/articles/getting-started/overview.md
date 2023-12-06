# Overview

Bytewax is an open source Python framework for building highly
scalable dataflows in a streaming or batch context.

Bytewax is a Python-native binding to the [Timely
Dataflow](https://github.com/TimelyDataflow/timely-dataflow) library.
Timely Dataflow is a distributed dataflow runtime written in
[Rust](https://www.rust-lang.org/).

# Benefits

At a high level, Bytewax provides a few major benefits:

- You can develop and execute your code locally, and then easily
  horizontally scale that code to a cluster or Kubernetes without
  changes.

- A self-hosted Kubernetes platform product that helps out with
  operational tasks like logging and metrics. And the
  [`waxctl`](https://bytewax.io/docs/deployment/waxctl) to help manage

- The Bytewax runtime provides a recovery system which automatically
  backs up the state in your dataflow and allows you to recover from
  failure without slowly re-processing all data.

- A built-in library of connectors to easily allow integration with
  external systems, and the ability to write your own connectors in
  pure Python.

- A built-in library of operators to handle common data transformation
  use cases, and the ability to write your own in pure Python.
