## Overview

Bytewax is a Python framework for building Dataflow programs over bounded or unbounded data.

Dataflow programming is fundamentally about describing your program as
independent components, each of which operate in response to the availability of
input data, as well as describing the connections between these components.

Bytewax is written in both Python and Rust. It is distributed as a Python library which makes use of
[PyO3](https://pyo3.rs/) to offer Python native bindings, and utilizes
[Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/) as a low-level
streaming computation framework.

With Bytewax, you can develop Dataflows locally, and deploy the same program on multiple workers
to process data in parallel.

Bytewax provides a number of important capabilities for writing Dataflows, including -

- Stateful Operators
- Recovery
- Windowing
- Joins
- And more!

Bytewax dataflows can be deployed on Kubernetes, as well as AWS and GCP instances through our command-line
utility, [waxctl](deployment/waxctl.md).

Bytewax also offers a [Platform](https://bytewax.io/platform) that simplifies
the management of stateful workloads on Kubernetes, and adds additional features
like single-sign on and disaster recovery.

## Where to go from here?

If you would like to learn more about how Dataflow programming works, or would like
to better understand how Bytewax works, see our [Concepts Section](/concepts).

Access to the Python API documentation can be found [here](/apidocs).

If you would like to make a contribution to Bytewax, please have a look at our
contribution guide [here](https://github.com/bytewax/bytewax/blob/main/CONTRIBUTING.md).

The source for Bytewax is hosted on [GitHub](https://github.com/bytewax/bytewax).
