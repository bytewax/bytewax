Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure _without_ re-processing
all initial data to re-calculate all internal state. It does this by
periodically snapshotting all internal state and having a way to
resume from a recent snapshot.

Here, we'll walk through a tutorial demonstrating recovery. For more advanced settings
and important details about recovery, see the [Concepts section for recovery](/docs/articles/concepts/recovery.md).

## Create Recovery Partitions

Recovery partitions must be pre-initialized before running the
dataflow. This is done by executing the `bytewax.recovery` module:

```shell
> python -m bytewax.recovery db_dir/ 1
```

This will create a recovery partition in the `db_dir/` directory:

```
$ ls db_dir/
part-0.sqlite3
```

## Executing with Recovery

Let's create an example dataflow that we can use to demonstrate recovery. We're going
to use the [stateful_map](/apidocs/bytewax.operators/index#bytewax.operators.stateful_map) operator
to keep a running sum of the numbers we receive as input.

`stateful_map` is, as the name implies, a stateful operator. `stateful_map` takes four
parameters: a `step_id`, a [Stream](/apidocs/bytewax.dataflow#bytewax.dataflow.Stream)
of input data, a **builder** function, and a **mapper** function.

The **mapper** function should return a 2-tuple of `(updated_state, emit_value)`. The `updated_state`
that is returned from this function is the internal state of this operator, and will be used
for recovery. The `emit_value` will be passed downstream.

Let's see a concrete example. Add the following code in a new file named `recovery.py`:

```python
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

inp = [0, 1, 2]

flow = Dataflow("recovery")
input_stream = op.input("inp", flow, TestingSource(inp))
# Stateful operators require their input to be keyed
# We'll use the static key "ALL" so that all input is
# processed together.
keyed_stream = op.key_on("key_stream", input_stream, lambda _: "ALL")


def update_sum(current_sum, new_item):
    updated_sum = current_sum + new_item
    return updated_sum, updated_sum


total_sum_stream = op.stateful_map("running_count", keyed_stream, lambda: 0, update_sum)
op.output("out", total_sum_stream, StdOutSink())
```

To enable recovery when you execute a dataflow, pass the `-r` flag to
`bytewax.run` and specify the recovery directory. We will also need to set
two values for recovery, the `snapshot_interval` and the `backup_interval`.

The `snapshot_interval` specifies the amount of time in seconds to wait
before creating a snapshot. The `backup_interval` specifies the amount
of time in seconds to keep older state snapshots around.

For production dataflows, you should set these values carefully for
each dataflow to match your operational needs. For more information, please
see the concept section on [recovery](/docs/articles/concepts/recovery.md).

Running the example above, you should see the following output:

```shell
> python -m bytewax.run recovery.py -r db_dir/ -s 30 -b 180
('ALL', 0)
('ALL', 1)
('ALL', 3)
```

Our dataflow stopped when it reached the end of our testing input, but importantly,
Bytewax has saved a snapshot of the state of the dataflow before exiting.

## Resuming

If a dataflow aborts, abruptly shuts down, or gracefully exits due to
reaching the end of input, you can resume the dataflow by running it again with the
files stored in the recovery directory.

Before we re-run our dataflow, let's change our input data to some new values:


```python
inp = [3, 4]
```

Now we can re-run our dataflow:

```shell
> python -m bytewax.run recovery.py -r db_dir/ -s 30 -b 180
('ALL', 6)
('ALL', 10)
```

You can see that the dataflow has restored the state of the `stateful_map` operator
from our previous run, and then applied our new data to that restored state.
