Bytewax allows a dataflow program to be run using multiple processes and/or threads, allowing you to scale your dataflow to take advantage of multiple cores on a single machine or multiple machines over the network.

A **worker** is a thread that is helping execute your dataflow. Workers can be grouped into separate **processes**, but refer to the individual threads within.

Bytewax's execution model uses identical workers. Workers execute all steps in a dataflow and automatically trade data to ensure the semantics of the operators. If a dataflow is run on multiple processes, there will be a slight overhead added to aggregating operators due to pickling and network communication.

Bytewax gives you a few entry points with different APIs and trade-offs in parallelism. Achieving the best performance of your dataflows requires considering the properties of your logic, input, and output. We'll give an overview and some hints here.

See our API docs or docstrings for a detailed description of arguments to these entry points.

## Single Worker Run

The simplest entry point is `bytewax.run_main()`. It will run your dataflow on a single worker in the current process. It has a simple iterator-based API takes an **input iterator** of `(epoch, item)` tuples and collects all output into a list of `(epoch, item)` tuples and returns it.

`run_main()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig

def input_builder(worker_index, worker_count, resume_state):
    state = None # Ignore recovery
    for i in range(3):
        yield (state, i)

def output_builder(worker_index, worker_count):
    def output_handler(item):
        print(item)
    return output_handler

flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(lambda item: item + 1)
flow.capture(ManualOutputConfig(output_builder))

run_main(flow)
```

```{testoutput}
1
2
3
```

`run_main()` is best used for testing dataflows or doing prototyping in notebooks.

Because it only uses a single worker, do not expect any speedup due to parallelism. `run_main()` collects all output into a list internally first, thus it will not support infinite, larger than memory datasets.

## Multiple Worker Spawn Cluster

Our next entry point introduces a more complex API to allow for more performance. `bytewax.spawn_cluster()` will spawn a number of background processes and threads on the local machine to run your dataflow. Input and output are individual to each worker, so there is no need to pass all input and output through the calling process. `spawn_cluster()` will block until the dataflow is complete.

### Input Configuration

You can manually configure your dataflow's input with an input builder and a `ManualInputConfig` or use a Kafka stream with `KafkaInputConfig`. You'll configure these with the `Dataflow.input` operator.

For a manual input source, you'll define an input **builder**, a function that is called on each worker and will produce the input for that worker. This input builder accepts three parameters: `worker_index, worker_count, resume_state`. The input builder function should return an iterable that yields a tuple of `(state, event)` and will be the `input_builder` parameter on the `ManualInputConfig` passed to the dataflow. The input builder must know how to skip ahead in its input data to start at that `resume_state`. The `resume_state` parameter allows you to configure recovery should a failure interrupt a stateful operation, such as `stateful_map`.

A `KafkaInputConfig` accepts four parameters: a list of brokers, a topic, a tail boolean and a starting_offset. See our API docs for `bytewax.inputs` for more on a Kafka configuration.

The per-worker input of `spawn_cluster()` has the extra requirement that the input data is not duplicated between workers. All data from each worker is introduced into the same dataflow. For example, if each worker reads the content of the same file, the input will be duplicated by the number of workers. Data sources like Apache Kafka or Redpanda can provide a partitioned stream of input which can be consumed by multiple workers in parallel without duplication, as each worker sees a unique partition.

### Output

Output is configured similarly to input, using a configuration and the `capture` operator. Akin to the `ManualInputConfig`, the `ManualOutputConfig` requires **builder** function that is called on each worker and will handle the output for that worker. However, the output builder function should return a callback **output handler** function that can be called with each item of output produced.

`spawn_cluster()` blocks until all output has been collected.

```python doctest:SORT_OUTPUT
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import ManualOutputConfig
from bytewax.testing import doctest_ctx

def input_builder(worker_index, worker_count, resume_state):
    resume_state = 0 # Ignore recovery for the moment
    for state, item in enumerate(range(resume_state, 3)):
        yield(state, {"Worker": worker_index, "Item": item})

def incr(item):
    item["Item"] += 1
    return item

def output_builder(worker_index, worker_count):
    def output_handler(item):
        print(item)
    return output_handler

flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(incr)
flow.capture(ManualOutputConfig(output_builder))
spawn_cluster(
    flow,
    proc_count=2,
    worker_count_per_proc=2,
    mp_ctx=doctest_ctx,  # Outside a doctest, you'd skip this.
)
```

```{testoutput}
{'Worker': 0, 'Item': 1}
{'Worker': 0, 'Item': 2}
{'Worker': 0, 'Item': 3}
{'Worker': 1, 'Item': 1}
{'Worker': 1, 'Item': 2}
{'Worker': 1, 'Item': 3}
{'Worker': 2, 'Item': 1}
{'Worker': 2, 'Item': 2}
{'Worker': 2, 'Item': 3}
{'Worker': 3, 'Item': 1}
{'Worker': 3, 'Item': 2}
{'Worker': 3, 'Item': 3}
```

Note how we have "duplicate" data because every worker runs the same input code.

This is best used when you have an input source that can be partitioned without coordination, like a Kafka topic or partitioned Parquet files or logs from individual servers, and need higher throughput as all steps (including input and output) are run in parallel.

## Multiple Workers Cluster Main

The final entry point, `bytewax.cluster_main()` is the most flexible. It allows you to start up a single process within a cluster of processes that you are manually coordinating. The input and output API also use operators and associated [builders](#builders), but you have to pass in the network addresses of the other processes you have started up yourself and assign them each a unique ID.

`cluster_main()` takes in a list of hostname and port of all workers (including itself). Each worker must be given a unique process ID.

As in `spawn_cluster()`, the input builder function passed to a manual input configuration should return an iterable that yields `(state, item)` tuples. If you are using a `KafkaInputConfig`, state recovery will be managed for you.

For a `ManualOutputConfig` passed to `capture`, the output builder function should return a callback function that can be called with each item of output produced.

`cluster_main()` blocks until the dataflow is complete.

```python doctest:SORT_OUTPUT
from bytewax.dataflow import Dataflow
from bytewax.execution import cluster_main
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import TestingOutputConfig, ManualOutputConfig
from bytewax.testing import doctest_ctx

def input_builder(worker_index, worker_count, resume_state):
    resume_state = 0 # Ignore recovery for the moment
    for state, item in enumerate(range(resume_state, 3)):
        yield(state, {"Worker": worker_index, "Item": item})

def incr(item):
    item["Item"] += 1
    return item

def output_builder(worker_index, worker_count):
    def output_handler(item):
        print(item)
    return output_handler

flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(incr)
flow.capture(ManualOutputConfig(output_builder))

addresses = [
    "localhost:2101",
    # You would put the address and port of each other process in the cluster here:
    # "localhost:2102",
    # ...
]
cluster_main(
    flow,
    addresses=addresses,
    proc_id=0,
    worker_count_per_proc=2
)
```

In this toy example here, the cluster is only of a single process with an ID of `0`, so there are only two workers in the sample output:

```{testoutput}
{'Worker': 0, 'Item': 1}
{'Worker': 0, 'Item': 2}
{'Worker': 0, 'Item': 3}
{'Worker': 1, 'Item': 1}
{'Worker': 1, 'Item': 2}
{'Worker': 1, 'Item': 3}
```

This is best used for using bytewax as a long-running data processing service. You can scale up to take advantage of multiple machines.

It still requires input and output to be partitioned (and workers to read non-overlapping input). There is overhead of pickling and sending items between processes when aggregation needs to occur.
