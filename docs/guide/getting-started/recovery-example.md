# Recovery Example

Bytewax allows you to **recover** a stateful dataflow; it will let you
resume processing and output due to a failure _without_ re-processing
all initial data to re-calculate all internal state. It does this by
periodically snapshotting all internal state and having a way to
resume from a recent snapshot.

Here, we'll walk through a tutorial demonstrating recovery. For more
advanced settings and important details about recovery, see the
concepts section article <project:#recovery>.

## Executing with Recovery

Let's create an example dataflow that we can use to demonstrate
recovery. We're going to use the
{py:obj}`~bytewax.operators.stateful_map` operator to keep a running
sum of the numbers we receive as input.

{py:obj}`~bytewax.operators.stateful_map` is, as the name implies, a
stateful operator. {py:obj}`~bytewax.operators.stateful_map` takes
four parameters: a `step_id`, a {py:obj}`~bytewax.dataflow.Stream` of
input data, a **builder** function, and a **mapper** function.

The **mapper** function should return a 2-tuple of `(updated_state,
emit_value)`. The `updated_state` that is returned from this function
is the internal state of this operator, and will be used for recovery.
The `emit_value` will be passed downstream.

Let's see a concrete example. Add the following code in a new file
named `recovery.py`:

```{testcode}
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
    if current_sum is None:
        current_sum = 0

    updated_sum = current_sum + new_item
    return updated_sum, updated_sum


total_sum_stream = op.stateful_map("running_count", keyed_stream, update_sum)
op.output("out", total_sum_stream, StdOutSink())
```

To enable recovery when you execute a dataflow, you need to pass some
parameters to {py:obj}`bytewax.run`.
- The `-d/--local-state-dir` flag to specify the directory where bytewax
  will write the database file for that process.
- The `-b/--backup` (optional) to indicate which `Backup` strategy to use for durable backup.
  This defaults to a `FileSystemBackup` instance saved in a subdirectory named `backup`
  in the same folder where the script is executed. You need to create that directory
  or pass a different one here. This flag works the same way as the dataflow import string,
  so you can use `--backup "bytewax.backup:file_system_backup('/tmp/bytewax/backup')"`.
- The `--snapshot-mode` (optional) to set when bytewax should take the snapshots. There are
  two modes: 'immediate' or 'batch'. In 'immediate' mode snapshots are taken as soon as
  the state changes, in 'batch' mode they are taken in batches at the end of the
  `snapshot_interval`. It's a tradeoff between latency and overall throughput. Taking
  snapshots in batch is usually more efficient overall, but can add a delay between
  snapshot intervals. Defaults to `immediate`.
- The `-s/--snapshot-interval` indicates when the recovery system should take snapshots if
  the `snapshot-mode` is `batch`, and when the `backup` class should be called to save the state
  into durable backup.

For production dataflows, you should set these values carefully for
each dataflow to match your operational needs. For more information,
please see the concept section on <project:#recovery>.

Running the example above, you should see the following output:

```console
$ mkdir backup
$ python -m bytewax.run recovery -d db_dir/ -s 30
```

```{testcode}
:hide:

from bytewax.testing import run_main

run_main(flow)
```

```{testoutput}
('ALL', 0)
('ALL', 1)
('ALL', 3)
```

Our dataflow stopped when it reached the end of our testing input, but
importantly, Bytewax has saved a snapshot of the state of the dataflow
before exiting.

## Resuming

If a dataflow aborts, abruptly shuts down, or gracefully exits due to
reaching the end of input, you can resume the dataflow by running it
again with the files stored in the recovery directory.

Before we re-run our dataflow, let's change our input data to add some
new values:


```{testcode}
inp.extend([3, 4])
```

Now we can re-run our dataflow:

```console
$ python -m bytewax.run recovery -d db_dir/ -s 30
('ALL', 6)
('ALL', 10)
```

You can see that the dataflow has restored the state of the
{py:obj}`~bytewax.operators.stateful_map` operator from our previous
run, started reading input from where it stopped, and then applied our
new data to that restored state.
