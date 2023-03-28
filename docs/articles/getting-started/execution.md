Bytewax allows a dataflow program to be run using multiple processes
and/or threads, allowing you to scale your dataflow to take advantage
of multiple cores on a single machine or multiple machines over the
network.

A **worker** is a thread that is helping execute your
dataflow. Workers can be grouped into separate **processes**, but
refer to the individual threads within.

Bytewax's execution model uses identical workers. Workers execute all
steps in a dataflow and automatically trade data to ensure the
semantics of the operators. If a dataflow is run on multiple
processes, there will be a slight overhead added to aggregating
operators due to pickling and network communication, but it will allow
you to read from input partitions in parallel for higher throughput.

Bytewax gives you two entry points with different APIs and
trade-offs in parallelism, and an helper script to run dataflows
using the appropriate entrypoint.
Achieving the best performance of your dataflows requires
considering the properties of your logic, input,
and output. We'll give an overview and some hints here.

See our API docs or docstrings for a detailed description of arguments
to these entry points.

## Single Worker Run

The first entry point is `bytewax.execution.run_main()`. It will
run your dataflow on a single worker in the current process.

`run_main()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.testing import TestingInput
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("inp", TestingInput(range(3)))
flow.map(lambda item: item + 1)
flow.output("out", StdOutput())

run_main(flow)
```

```{testoutput}
1
2
3
```

If you intend to run the dataflow on a single process, using
`run_main()` will avoid the initial overhead of setting up the
communication between processes.

Otherwise, `run_main()` is best used for testing dataflows or doing
prototyping in notebooks.

Because it only uses a single worker, do not expect any speedup from
to parallelism.

## Multiple Workers Cluster Main

The next entry point, `bytewax.execution.cluster_main()` is the most
flexible. It allows you to start up a single process within a cluster
of processes that you are manually coordinating. We recommend you
checkout the documentation for [waxctl](/docs/deployment/waxctl/) our
command line tool which facilitates running a dataflow on Kubernetes.

`cluster_main()` takes in a list of hostname and port of all workers
(including itself). Each worker must be given a unique process ID.

`cluster_main()` blocks until the dataflow is complete.

```python doctest:SORT_OUTPUT
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.connectors.stdio import StdOutput
from bytewax.testing import doctest_ctx, TestingInput

flow = Dataflow()
flow.input("inp", TestingInput(range(3)))
flow.map(lambda item: item + 1)
flow.output("out", StdOutput())

addresses = [
    "localhost:2101",
    # You would put the address and port of each other process in the cluster here:
    # "localhost:2102",
    # ...
]
cluster_main(flow, addresses=addresses, proc_id=0, worker_count_per_proc=2)
```

```{testoutput}
1
2
3
```

This is best used for using bytewax as a long-running data processing
service. You can scale up to take advantage of multiple machines.

## Multiple Worker Run Script

The final entry point is `bytewax.run`, which is a run script
that can spawn multiple processes, each running a `cluster_main` with the
appropriate parameters automatically generated.

`bytewax.run` will spawn a number of background processes and threads
on the local machine to run your dataflow.
With the run script, the input and output will be
partitioned between the workers and the call will block until the
dataflow is complete.

To use it, rather than calling the dataflow with `python dataflow.py`,
you pass the python file as an argument to `bytewax.execution.run`:

```
python -m bytewax.run dataflow.py
```

See `python -m bytewax.run --help` for all the possible options.

If you manually call `run_main` inside the file, it will be ignored
when executing it with `bytewax.run`:

```python doctest:SORT_OUTPUT
from bytewax.execution import run_main
from bytewax.dataflow import Dataflow
from bytewax.testing import doctest_ctx, TestingInput
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("inp", TestingInput(range(3)))
flow.map(lambda item: item + 1)
flow.output("out", StdOutput())

# This will be called but won't run the dataflow, the script will.
run_main(flow)
```

```{testoutput}
1
2
3
```
