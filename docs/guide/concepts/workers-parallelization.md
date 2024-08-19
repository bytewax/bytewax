# Workers and Parallelization

## Execution Model

A **worker** is a thread that is helping execute your dataflow.
Workers can be grouped into separate **processes**, but refer to the
individual threads within. A **cluster** is a set of processes that
have been configured to collaborate on running a dataflow.

If a dataflow is run on multiple processes, there will be a slight
overhead due to pickling and network communication whenever items must
be moved between workers, but it will allow you to parallelize some
work for higher throughput. See <project:#xref-operator-execution> and
the {py:obj}`~bytewax.operators.redistribute` operator for more
information.

## Run Script

(xref-flow-arg)=
### Specifying the Dataflow

The first argument passed to the script is a dataflow getter string.
The string is in the format `<dataflow-module>[:<dataflow-getter>]`.

- `<dataflow-module>` points to the Python module containing the
  dataflow.

- `<dataflow-getter>` is either the name of a Python variable with a
  {py:obj}`~bytewax.dataflow.Dataflow` instance, or a function call to
  a function defined in the module. If missing, this defaults to
  looking for the variable named `flow`.

```console
$ python -m bytewax.run examples.simple
```

For example, if you are at the root of this repository, you can run
the "simple.py" example by calling the script with the following
argument:

```console
$ python -m bytewax.run examples.simple:flow
```

If instead of a variable, you have a function that returns a dataflow,
you can use a string after the `:` to call the function, possibly with
args:


```console
$ python -m bytewax.run "my_dataflow:get_flow('/tmp/file')"
```

By default this script will run a single worker on a single process.

## Single Worker Run

By default {py:obj}`bytewax.run` will run your dataflow on a single
worker in the current process. This avoids the overhead of setting up
communication between workers/processes, but the dataflow will not
have any gain from parallelization.

```console
$ python -m bytewax.run examples.simple
```

## Single Process Cluster

By changing the `-w/--workers-per-process` argument, you can spawn
multiple workers within a single process.

For example you can run the previous dataflow with 3 workers by
changing only the command:

```console
$ python -m bytewax.run -w3 examples.simple
```

## Multi-Process Cluster

If you want to run multiple processes on a single machine, or
different machines on the same network, you can use the
`-i/--process-id`,`-a/--addresses` parameters.

Each invocation of {py:obj}`bytewax.run` with `-i` starts up a single
process. By executing this command multiple times, you can create a
cluster of Bytewax processes on one machine or multiple machines. We
recommend you checkout the documentation for [`waxctl`](#xref-waxctl)
our command line tool which facilitates running a multiple dataflow
processes locally, or on Kubernetes.

The `-a/--addresses` parameter represents a list of addresses for all
the processes, separated by a ';'. When you run single processes
separately, you need to assign a unique id to each process. The
`-i/--process-id` should be a number starting from `0` representing
the position of its respective address in the list passed to `-a`.

For example you want to run 2 processes, with 3 workers each, on two
different machines. The machines are known in the network as
`cluster_one` and `cluster_two`. You should run the first process on
`cluster_one` as follows:

```console
$ python -m bytewax.run simple:flow -w3 -i0 -a "cluster_one:2101;cluster_two:2101"
```

And on the `cluster_two` machine as:

```console
$ python -m bytewax.run simple:flow -w3 -i1 -a "cluster_one:2101;cluster_two:2101"
```
