Bytewax allows a dataflow to be run using multiple processes and/or
threads, allowing you to scale your dataflow to take advantage of multiple cores
on a single machine or multiple machines over the network.

Bytewax does not require an additional process to run a dataflow, or to
coordinate multiple processes on different machines.

## Workers

A worker is a thread that is executing your dataflow. Workers can be
grouped into separate processes, but refer to the individual threads within.

Bytewax's execution model uses identical workers. Workers execute all steps in a
dataflow and automatically exchange data to ensure the semantics of the operators.

## Running a dataflow

Bytewax includes a module that is used to run dataflows. To see all of the
available runtime options, run the following command:

```shell
> python -m bytewax.run --help
```

## Selecting the dataflow

The first argument passed to the script is a dataflow getter string.

The string is in the format `<dataflow-module>:<dataflow-getter>` where:
- `<dataflow-module>` points to a Python [module](https://docs.python.org/3/tutorial/modules.html)
   containing the dataflow definition.
- `<dataflow-getter>` is either the name of a Python variable holding the flow, or a function
   call to a function defined in the module.

Let's work through two examples. Make sure you have [installed Bytewax](/docs/articles/getting-started/installation.md)
before you begin.

Create a new file named `./simple.py` with the following contents:

```python
# ./simple.py
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("simple")
inp = op.input("inp", flow, TestingSource(range(3)))
plus_one = op.map("plus_one", inp, lambda item: item + 1)
op.output("out", plus_one, StdOutSink())
```

To run this flow, you can use `simple:flow`:

```shell
> python -m bytewax.run simple:flow
```

In this case, you can even elide the `:flow` part of the dataflow getter string,
as Bytewax will default to running a dataflow stored in a variable named `flow`:

```shell
> python -m bytewax.run simple
```

## Parameterizing a dataflow

In some cases you might want to change the Dataflow's behaviour
without having to change the source code.

As an example, what if you want to test the previous Dataflow with different
input ranges? To accomplish this, you can rewrite the file so that the Dataflow
is built from a function, rather than being defined as a variable in the file.

Create a new file named `parametric.py` with the following contents:

```python
# ./parametric.py
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink


def get_flow(input_range):
    flow = Dataflow("parametric")
    inp = op.input("inp", flow, TestingSource(range(input_range)))
    plus_one = op.map("plus_one", inp, lambda item: item + 1)
    op.output("out", plus_one, StdOutSink())
    return flow
```

You can now run your dataflow with parameters that are passed
to the `get_flow` function:

```shell
> python -m bytewax.run "parametric:get_flow(10)"
```

## Single Worker Run

By default `bytewax.run` will run your dataflow on a single worker
in the current Python process.

This avoids the overhead of setting up communication between workers/processes,
but the dataflow will not have any gain from parallelization.

By changing the `-w/--workers-per-process` arguments,
you can spawn mulitple workers per process. We can run the previous dataflow
with 3 workers using the same file, changing only the command:

```shell
> python -m bytewax.run -w3 simple:flow
```

## Clustering Bytewax processes

If you want to run Bytewax processes on one or more machines on the same network,
you can use the `-i/--process-id`,`-a/--addresses` parameters.

The `-i` and `-a` flags allow you to start up a single process within a cluster
of processes that you are manually coordinating.

The `-a/--addresses` parameter is a list of `addresses:port` entries for all the processes,
each entry separated by a ';'.

When you run single processes separately, you need to assign a unique id to each process.
The `-i/--process-id` should be a number starting from `0` representing the position
of its respective address in the list passed to `-a`.

If for example you want to run 2 workers, on 2 different machines
where the machines are known via DNS in the network as `cluster_one` and `cluster_two`,
you should run the first process on `cluster_one` as follows:

```shell
> python -m bytewax.run simple:flow -i0 -a "cluster_one:2101;cluster_two:2101"
```

And on the `cluster_two` machine as:

```shell
> python -m bytewax.run simple:flow -i1 -a "cluster_one:2101;cluster_two:2101"
```

As before, each process can start multiple workers with the `-w` flag for increased
parallelism. To start the same dataflow with a total of 6 workers:

```shell
> python -m bytewax.run simple:flow -w3 -i0 -a "cluster_one:2101;cluster_two:2101"
```

And on the `cluster_two` machine as:

```shell
> python -m bytewax.run simple:flow -w3 -i1 -a "cluster_one:2101;cluster_two:2101"
```

For more information about deployment options for Bytewax dataflows, please see
the documentation for [waxctl](/docs/deployment/waxctl/) our command line tool which
facilitates deploying a dataflow.
