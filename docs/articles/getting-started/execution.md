Bytewax allows a dataflow program to be run using multiple processes and/or threads, allowing you to scale your dataflow to take advantage of multiple cores on a single machine or multiple machines over the network.

A **worker** is a thread that is helping execute your dataflow. Workers can be grouped into separate **processes**, but refer to the individual threads within.

Bytewax's execution model uses identical workers. Workers execute all steps in a dataflow and automatically trade data to ensure the semantics of the operators. If a dataflow is run on multiple processes, there will be a slight overhead added to aggregating operators due to pickling and network communication.

Bytewax gives you a few entry points with different APIs and trade-offs in parallelism. Achieving the best performance of your dataflows requires considering the properties of your logic, input, and output. We'll give an overview and some hints here.

See our API docs or docstrings for a detailed description of arguments to these entry points.

## Single Worker Run

The simplest entry point is `bytewax.run()`. It will run your dataflow on a single worker in the current process. It has a simple iterator-based API takes an **input iterator** of `(epoch, item)` tuples and collects all output into a list of `(epoch, item)` tuples and returns it.

`run()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax import Dataflow, run


def input():
    for epoch, item in enumerate(range(3)):
        yield epoch, item


flow = Dataflow()
flow.map(lambda item: item + 1)
flow.capture()


for epoch, item in run(flow, input()):
    print(epoch, item)
```

```{testoutput}
0 1
1 2
2 3
```

`run()` is best used for testing dataflows or doing prototyping in notebooks.

Because it only uses a single worker, do not expect any speedup due to parallelism. `run()` collects all output into a list internally first, thus it will not support infinite, larger than memory datasets.

## Multiple Worker Run Cluster

Our first entry point that enables any parallelism is `bytewax.run_cluster()`. It will spawn a number of background processes on the local machine with a number of worker threads each to run your dataflow. It handles coordination between all the processes for you.

Input and output are identical to `run()`. `run_cluster()` takes an input generator of `(epoch, item)` tuples and collects all output into a list of `(epoch, item)` tuples and returns it.

`run_cluster()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax import run_cluster
from bytewax.testing import doctest_ctx


def input():
    for epoch, item in enumerate(range(3)):
        yield epoch, item


flow = Dataflow()
flow.map(lambda item: item + 1)
flow.capture()


for epoch, item in run_cluster(
    flow,
    input(),
    proc_count=2,
    worker_count_per_proc=2,
    mp_ctx=doctest_ctx,  # Outside a doctest, you'd skip this.
):
    print(epoch, item)
```

```{testoutput}
0 1
1 2
2 3
```

This is best used for notebook analysis where you need higher throughput or parallelism, or simple stand-alone demo programs.

`run_cluster()` has some constraints, though. Because of the Python iterator-based API, all input and output must be routed through the calling Python process, thus only the internal steps of the dataflow are executed in parallel. `run_cluster()` is best suited for speeding up CPU-bound single-threaded Python processing limited, not IO-bound processing. Operator logic that calls into already parallel code in NumPy or Pandas will probably not see throughput speedup as the internal parallelism will already fill available CPU time without Bytewax's parallelism.

## Multiple Worker Spawn Cluster

Our next entry point introduces a more complex API to allow for more performance. `bytewax.spawn_cluster()` will spawn a number of background processes and threads on the local machine to run your dataflow. Input and output are individual to each worker, so there is no need to pass all input and output through the calling process. `spawn_cluster()` will block until the dataflow is complete.

### Builders

You provide input and output **builders**, functions that are called on each worker and will produce the input and handle the output for that worker. The input builder function should return an iterable that yields `Emit(item)` or `AdvanceTo(epoch)`. The output builder function should return a callback **output handler** function that can be called with each epoch and item of output produced.

The input builder also recieves the epoch to resume processing on in the case of restarting the dataflow. The input builder must somehow skip ahead in its input data to start at that epoch.

The per-worker input of `spawn_cluster()` has the extra requirement that the input data is not duplicated between workers. All data from each worker is introduced into the same dataflow. For example, if each worker reads the content of the same file, the input will be duplicated by the number of workers. Data sources like Apache Kafka or Redpanda can provide a partitioned stream of input which can be consumed by multiple workers in parallel without duplication, as each worker sees a unique partition.

`spawn_cluster()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax import spawn_cluster, AdvanceTo, Emit
from bytewax.testing import test_print


def input_builder(worker_index, worker_count, resume_epoch):
    for epoch, x in enumerate(range(resume_epoch, 3)):
        yield AdvanceTo(epoch)
        yield Emit({"input_from": worker_index, "x": x})


def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, item = epoch_item
        test_print("Epoch:", epoch, "Worker", item["input_from"], "introduced:", item["x"])

    return output_handler


def incr(item):
    item["x"] += 1
    return item


flow = Dataflow()
flow.map(incr)
flow.capture()


spawn_cluster(
    flow,
    input_builder,
    output_builder,
    proc_count=2,
    worker_count_per_proc=2,
    mp_ctx=doctest_ctx,  # Outside a doctest, you'd skip this.
)
```

```{testoutput}
Epoch: 0 Worker 0 introduced: 1
Epoch: 0 Worker 1 introduced: 1
Epoch: 0 Worker 2 introduced: 1
Epoch: 0 Worker 3 introduced: 1
Epoch: 1 Worker 0 introduced: 2
Epoch: 1 Worker 1 introduced: 2
Epoch: 1 Worker 2 introduced: 2
Epoch: 1 Worker 3 introduced: 2
Epoch: 2 Worker 0 introduced: 3
Epoch: 2 Worker 1 introduced: 3
Epoch: 2 Worker 2 introduced: 3
Epoch: 2 Worker 3 introduced: 3
```

Note how we have "duplicate" data because every worker runs the same input code.

This is best used when you have an input source that can be partitioned without coordination, like a Kafka topic or partitioned Parquet files or logs from individual servers, and need higher throughput. Unlike `run_cluster()`, all steps (including input and output) are run in parallel.

## Multiple Workers Manual Cluster

The final entry point, `bytewax.cluster_main()` is the most flexible. It allows you to start up a single process within a cluster of processes that you are manually coordinating. The input and output API also use [builders](#builders), but you have to pass in the network addresses of the other processes you have started up yourself and assign them each a unique ID.

`cluster_main()` takes in a list of hostname and port of all workers (including itself). Each worker must be given a unique process ID.

The input builder function should return an iterable that yields `(epoch, item)` tuples. The output builder function should return a callback function that can be called with each epoch and item of output produced.

`cluster_main()` blocks until the dataflow is complete.

```python doctest:SORT_OUTPUT
from bytewax import cluster_main


def input_builder(worker_index, worker_count, resume_epoch):
    for epoch, x in enumerate(range(resume_epoch, 3)):
        yield AdvanceTo(epoch)
        yield Emit({"input_from": worker_index, "x": x})


def output_builder(worker_index, worker_count):
    def output_handler(epoch_item):
        epoch, item = epoch_item
        test_print("Epoch:", epoch, "Worker", item["input_from"], "introduced:", item["x"])

    return output_handler


flow = Dataflow()


def incr(item):
    item["x"] += 1
    return item


flow.map(incr)
flow.capture()


addresses = [
    "localhost:2101",
    # You would put the address and port of each other process in the cluster here:
    # "localhost:2102",
    # ...
]
cluster_main(flow, input_builder, output_builder, addresses=addresses, proc_id=0, worker_count_per_proc=2)
```

In this toy example here, the cluster is only of a single process with an ID of `0`, so there are only two workers in the sample output:

```{testoutput}
Epoch: 0 Worker 0 introduced: 1
Epoch: 0 Worker 1 introduced: 1
Epoch: 1 Worker 0 introduced: 2
Epoch: 1 Worker 1 introduced: 2
Epoch: 2 Worker 0 introduced: 3
Epoch: 2 Worker 1 introduced: 3
```

This is best used for using Bytewax as a long-running data processing service. You can scale up to take advantage of multiple machines.

It still requires input and output to be partitioned (and workers to read non-overlapping input). There is overhead of pickling and sending items between processes when aggregation needs to occur.
