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

Achieving the best performance of your dataflows requires
considering the properties of your logic, input,
and output. We'll give an overview and some hints here.

Bytewax gives you a script that can be used to run dataflows:

```
python -m bytewax.run --help
```

## Selecting the dataflow

The first argument passed to the script is a dataflow getter string.
The string is in the format `<dataflow-module>:<dataflow-getter>`.
- `<dataflow-module>` points to a python module containing the dataflow definition
- `<dataflow-getter>` is either the name of a python variable holding the flow, or a function call to a function defined in the module

Let's see two examples (we assume bytewax is correctly installed).

Write your dataflow to a file named `./simple.py`:

```python
# ./simple.py
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("inp", TestingInput(range(3)))
flow.map(lambda item: item + 1)
flow.output("out", StdOutput())
```

To run this flow, you should use `simple:flow`:
```
python -m bytewax.run simple:flow
```

In some cases you might want to change the Dataflow's behaviour
without having to change the source code.

What if you want to test the previous Dataflow with different
input ranges?

You can rewrite the file so that the Dataflow is built from a function,
rather than being defined in the file:

```python
# ./parametric.py
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput
from bytewax.connectors.stdio import StdOutput

def get_flow(input_range):
    flow = Dataflow()
    flow.input("inp", TestingInput(range(input_range)))
    flow.map(lambda item: item + 1)
    flow.output("out", StdOutput())
    return flow
```

And you can run it with:
```
$ python -m bytewax.run "parametric:get_flow(10)"
```

The Dataflow can run in different ways, depending on the arguments
you pass to the entrypoint.

Let's explore them!

## Single Worker Run

By default `bytewax.run` will run your dataflow on a single worker
in the current process.
This avoids the overhead of setting up communication between workers/processes,
but the dataflow will not have any gain from parallelization.

As you saw in the previous chapter, you just need to pass the Dataflow getter string:
```
$ python -m bytewax.run simple:flow
```

Bytewax exposes the internal functions used to run the dataflow instance,
but those should only be used in a testing/experimental setup:

```python doctest:SORT_OUTPUT
# At the end of the file:
from bytewax.testing import run_main
run_main(flow)
```

```{testoutput}
1
2
3
```

## Local Cluster

By changing the `-p/--processes` and `-w/--workers-per-process` arguments,
you can spawn multiple processes, and mulitple workers per process,
letting `bytewax.run` handle the communication between them.

For example you can run the previous dataflow with 2 processes, and 3 workers
per process, for a total of 6 workers using the exact same file, changing
only the command:

```
$ python -m bytewax.run -p2 -w3 simple:flow
```

The input and outputs will be partitioned between the workers and the process
will run until the dataflow is complete.

## Manually Handled Cluster

If you want to run single processes on possibly different machines on the same network,
you can use the `-i/--process-id`,`-a/--addresses` parameters.

It allows you to start up a single process within a cluster
of processes that you are manually coordinating. We recommend you
checkout the documentation for [waxctl](/docs/deployment/waxctl/) our
command line tool which facilitates running a dataflow on Kubernetes.


The `-a/--addresses` parameter represents a list of addresses for all the processes,
separated by a ';'.
When you run single processes separately, you need to assign a unique id to each process.
The `-i/--process-id` should be a number starting from `0` representing the position
of its respective address in the list passed to `-a`.

For example you want to run 2 processes, with 3 workers each, on two different machines.
The machines are known in the network as `cluster_one` and `cluster_two`.
You should run the first process on `cluster_one` as follows:
```
$ python -m bytewax.run simple:flow -w3 -i0 -a "cluster_one:2101;cluster_two:2101"
```

And on the `cluster_two` machine as:
```
$ python -m bytewax.run simple:flow -w3 -i1 -a "cluster_one:2101;cluster_two:2101"
```

This is only needed if you want to run the dataflow on multiple machines,
or if you need better control of the addresses/ports used by default by the run script.
