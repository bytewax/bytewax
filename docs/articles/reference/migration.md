(migration)=
# Migration Guide

This guide can help you upgrade code through breaking changes from one
Bytewax version to the next. For a detailed list of all changes, see
the
[CHANGELOG](https://github.com/bytewax/bytewax/blob/main/CHANGELOG.md).

## From v0.17 to v0.18

### Non-Linear Dataflows

With the addition of non-linear dataflows, the API for constructing
dataflows has changed. Operators are now stand-alone functions that
can take and return streams.

 All operators, not just stateful ones, now require a `step_id`; it
 should be a `"snake_case"` description of the semantic purpose of
 that dataflow step.

 Also instantiating the dataflow itself now takes a "dataflow ID" so
 you can disambiguate different dataflows in the metrics.

Before:

```python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("inp", TestingInput(range(10)))
flow.map(lambda x: x + 1)
flow.output("out", StdOutput())
```

After:

```python
import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("basic")
# `op.input` takes the place of `flow.input` and takes a `Dataflow`
# object as the first parameter.
# `op.input` returns a `Stream[int]` in this example:
stream = op.input("inp", flow, TestingSource(range(10)))
# `op.map` takes a `Stream` as it's second argument, and
# returns a new `Stream[int]` as it's return value.
add_one_stream = op.map("add_one", stream, lambda x: x + 1)
# `op.output` takes a stream as it's second argument, but
# does not return a new `Stream`.
op.output("out", add_one_stream, StdOutSink())
```

### Kafka/RedPanda Input

{py:obj}`~bytewax.connectors.kafka.KafkaSource` has been updated.
{py:obj}`~bytewax.connectors.kafka.KafkaSource` now returns a stream
of {py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage` dataclasses,
and a stream of errors rather than a stream of (key, value) tuples.

You can still use {py:obj}`~bytewax.connectors.kafka.KafkaSource` and
{py:obj}`~bytewax.connectors.kafka.KafkaSink` directly, or use the
custom operators in {py:obj}`bytewax.connectors.kafka` to construct an
input source.

Before:

```python doctest:SKIP
from bytewax.connectors.kafka import KafkaInput, KafkaOutput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow

flow = Dataflow()
flow.input("inp", KafkaInput(["localhost:9092"], ["input_topic"]))
flow.output(
    "out",
    KafkaOutput(
        brokers=["localhost:9092"],
        topic="output_topic",
    ),
)
```

After:

```python
from typing import Tuple, Optional

from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.dataflow import Dataflow

flow = Dataflow("kafka_in_out")
kinp = kop.input("inp", flow, brokers=["localhost:19092"], topics=["in_topic"])
in_msgs = op.map("get_k_v", kinp.oks, lambda msg: (msg.key, msg.value))


def wrap_msg(k_v):
    k, v = k_v
    return KafkaSinkMessage(k, v)


out_msgs = op.map("wrap_k_v", in_msgs, wrap_msg)
kop.output("out1", out_msgs, brokers=["localhost:19092"], topic="out_topic")
```

### Renaming

We have renamed the IO classes to better match their semantics and the
way they are talked about in the documentation. Their functionality
are unchanged. You should be able to search and replace for the old
names and have them work correctly.

| Old | New |
|-----|-----|
| `Input` | `Source` |
| `PartitionedInput` | `FixedPartitionedSource` |
| `DynamicInput` | `DynamicSource` |
| `StatefulSource` | `StatefulSourcePartition` |
| `StatelessSource` | `StatelessSourcePartition` |
| `SimplePollingInput` | `SimplePollingSource` |
| `Output` | `Sink` |
| `PartitionedOutput` | `FixedPartitionedSink` |
| `DynamicOutput` | `DynamicSink` |
| `StatefulSink` | `StatefulSinkPartition` |
| `StatelessSink` | `StatelessSinkPartition` |

We have also updated the names of the built-in connectors to match
this new naming scheme.

| Old | New |
|-----|-----|
| `FileInput` | `FileSource` |
| `FileOutput` | `FileSink` |
| `DirInput` | `DirSource` |
| `DirOutput` | `DirSink` |
| `CSVInput` | `CSVSource` |
| `StdOutput` | `StdOutSink` |
| `KafkaInput` | `KafkaSource` |
| `KafkaOutput` | `KafkaSink` |
| `TestingInput` | `TestingSource` |
| `TestingOutput` | `TestingSink` |

### Current Time Convenience

In addition to the name changes, we have also added a `datetime` argument to
`build{_part,}` with the current time to allow you to easily
setup a next awake time. You can ignore this parameter if you are not scheduling
awake times.

We have also added a `datetime` argument to `next_batch`, which contains the
scheduled awake time for that source. Since awake times are scheduled, but not
guaranteed to fire at the precise time specified, you can use this parameter
to account for any difference.


### `SimplePollingSource` moved

{py:obj}`~bytewax.inputs.SimplePollingSource` has been moved from
`bytewax.connectors.periodic` to {py:obj}`bytewax.inputs`. You'll need
to change your imports if you are using that class.

Before:

```python doctest:SKIP
from bytewax.connectors.periodic import SimplePollingSource
```

After:

```python
from bytewax.inputs import SimplePollingSource
```

### Window Metadata

Window operators now emit
{py:obj}`~bytewax.operators.window.WindowMetadata` objects downstream.
These objects can be used to introspect the open_time and close_time
of windows. This changes the output type of windowing operators from a
stream of: `(key, values)` to a stream of `(key, (metadata, values))`.

### Recovery flags

The default values for `snapshot-interval` and `backup-interval` have been removed
when running a dataflow with recovery enabled.

Previously, the defaults values were to create a snapshot every 10 seconds and
keep a day's worth of old snapshots. This means your recovery DB would max out at a size on disk
theoretically thousands of times bigger than your in-memory state.

See <project:/articles/concepts/recovery.md> for how to appropriately
pick these values for your deployment.

### Batch -> Collect

In v0.18, we've renamed the `batch` operator to
{py:obj}`~bytewax.operators.collect` so as to not be confused with
runtime batching. Behavior is unchanged.

## From v0.16 to v0.17

Bytewax v0.17 introduces major changes to the way that recovery works
in order to support rescaling. In v0.17, the number of workers in a
cluster can now be changed by stopping the dataflow execution and
specifying a different number of workers on resume.

In addition to rescaling, we've changed the Bytewax inputs and outputs
API to support batching which has yielded significant performance
improvements.

In this article, we'll go over the updates we've made to our API and
explain the changes you'll need to make to your dataflows to upgrade
to v0.17.

### Recovery changes

In v0.17, recovery has been changed to support rescaling the number of
workers in a dataflow. You now pre-create a fixed number of SQLite
recovery DB files before running the dataflow.

SQLite recovery DBs created with versions of Bytewax prior to v0.17
are not compatible with this release.

#### Creating recovery partitions

Creating recovery stores has been moved to a separate step from
running your dataflow.

Recovery partitions must be pre-initialized before running the
dataflow initially. This is done by executing this module:

``` bash
$ python -m bytewax.recovery db_dir/ 4
```


This will create a set of partitions:

``` bash
$ ls db_dir/
part-0.sqlite3
part-1.sqlite3
part-2.sqlite3
part-3.sqlite3
```

Once the recovery partition files have been created, they must be
placed in locations that are accessible to the workers. The cluster
has a whole must have access to all partitions, but any given worker
need not have access to any partition in particular (or any at
all). It is ok if a given partition is accesible by multiple workers;
only one worker will use it.

If you are not running in a cluster environment but on a single
machine, placing all the partitions in a single local filesystem
directory is fine.

Although the partition init script will not create these, partitions
after execution may consist of multiple files:

```
$ ls db_dir/
part-0.sqlite3
part-0.sqlite3-shm
part-0.sqlite3-wal
part-1.sqlite3
part-2.sqlite3
part-3.sqlite3
part-3.sqlite3-shm
part-3.sqlite3-wal
```
You must remember to move the files with the prefix `part-*.` all together.

#### Choosing the number of recovery partitions

An important consideration when creating the initial number of
recovery partitions; When rescaling the number of workers, the number
of recovery partitions is currently fixed.

If you are scaling up the number of workers in your cluster to
increase the total throughput of your dataflow, the work of writing
recovery data to the recovery partitions will still be limited to the
initial number of recovery partitions. If you will likely scale up
your dataflow to accomodate increased demand, we recommend that that
you consider creating more recovery partitions than you will initially
need. Having multiple recovery partitions handled by a single worker
is fine.

### Epoch interval -> Snapshot interval

We've renamed the cli option of `epoch-interval` to
`snapshot-interval` to better describe its affect on dataflow
execution. The snapshot interval is the system time duration (in
seconds) to snapshot state for recovery.

Recovering a dataflow can only happen on the boundaries of the most
recently completed snapshot across all workers, but be aware that
making the `snapshot-interval` more frequent increases the amount of
recovery work and may impact performance.

### Backup interval, and backing up recovery partitions.

We've also introduced an additional parameter to running a dataflow:
`backup-interval`.

When running a Dataflow with recovery enabled, it is recommended to
back up your recovery partitions on a regular basis for disaster
recovery.

The `backup-interval` parameter is the length of time to wait before
"garbage collecting" older snapshots. This enables a dataflow to
successfully resume when backups of recovery partitions happen at
different times, which will be true in most distributed deployments.

This value should be set in excess of the interval you can guarantee
that all recovery partitions will be backed up to account for
transient failures. It defaults to one day.

If you attempt to resume from a set of recovery partitions for which
the oldest and youngest backups are more than the backup interval
apart, the resumed dataflow could have corrupted state.

### Input and Output changes

In v0.17, we have restructured input and output to support batching
for increased throughput.

If you have created custom input connectors, you'll need to update
them to use the new API.

### Input changes

The `list_parts` method has been updated to return a `List[str]` instead of
a `Set[str]`, and now should only reflect the available input partitions
that a given worker has access to. You no longer need to return the
complete set of partitions for all workers.

The `next` method of `StatefulSource` and `StatelessSource` has been
changed to `next_batch` and should to return a `List` of elements each
time it is called. If there are no elements to return, this method
should return the empty list.

#### Next awake

Input sources now have an optional `next_awake` method which you can
use to schedule when the next `next_batch` call should occur. You can
use this to "sleep" the input operator for a fixed amount of time
while you are waiting for more input.

The default behavior uses a simple heuristic to prevent a spin loop
when there is no input. Always use `next_awake` rather than using a
`time.sleep` in an input source.

See the `periodic_input.py` example in the examples directory for an
implementation that uses this functionality.

### Async input adapter

We've included a new `bytewax.inputs.batcher_async` to help you use
async Python libraries in Bytewax input sources. It lets you wrap an
async iterator and specify a maximum time and size to collect items
into a batch.

### Using Kafka for recovery is now removed

v0.17 removes the deprecated `KafkaRecoveryConfig` as a recovery store
to support the ability to rescale the number of workers.

### Support for additional platforms

Bytewax is now available for linux/aarch64 and linux/armv7.

## From 0.15 to 0.16

Bytewax v0.16 introduces major changes to the way that custom Bytewax
inputs are created, as well as improvements to windowing and
execution. In this article, we'll cover some of the updates we've made
to our API to make upgrading to v0.16 easier.

### Input changes

In v0.16, we have restructured input to be based around partitions in
order to support rescaling stateful dataflows in the future. We have
also dropped the `Config` suffix to our input classes. As an example,
`KafkaInputConfig` has been renamed to `KafkaInput` and has been moved
to `bytewax.connectors.kafka.KafkaInput`.

`ManualInputConfig` is now replaced by two base superclasses that can
be subclassed to create custom input sources. They are `DynamicInput`
and `PartitionedInput`.

#### `DynamicInput`

`DynamicInput` sources are input sources that support reading distinct
items from any number of workers concurrently.

`DynamicInput` sources do not support resume state and thus generally
do not provide delivery guarantees better than at-most-once.

### `PartitionedInput`

`PartitionedInput` sources can keep internal state on the current
position of each partition. If a partition can "rewind" and resume
reading from a previous position, they can provide delivery guarantees
of at-least-once or better.

`PartitionedInput` sources maintain the state of each source and
re-build that state during recovery.

### Deprecating Kafka Recovery

In order to better support rescaling Dataflows, the Kafka recovery
store option is being deprecated and will be removed from a future
release in favor of the SQLite recovery store.

### Capture -> Output

The `capture` operator has been renamed to `output` in v0.16 and is
now stateful, so requires a step ID:

In v0.15 and before:

``` python doctest:SKIP
flow.capture(StdOutputConfig())
```

In v0.16+:

``` python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.output("out", StdOutput())
```

`ManualOutputConfig` is now replaced by two base superclasses that can
be subclassed to create custom output sinks. They are `DynamicOutput`
and `PartitionedOutput`.

### New entrypoint

In Bytewax v0.16, the way that Dataflows are run has been simplified,
and most execution functions have been removed.

Similar to other Python frameworks like Flask and FastAPI, Dataflows
can now be run using a Datfaflow import string in the
`<module_name>:<dataflow_variable_name_or_factory_function>` format.
As an example, given a file named `dataflow.py` with the following
contents:

``` python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("in", TestingInput(range(3)))
flow.output("out", StdOutput())
```

Since a Python file `dataflow.py` defines a module `dataflow`, the
Dataflow can be run with the following invocation:

``` bash
> python -m bytewax.run dataflow
0
1
2
```

By default, Bytewax looks for a variable in the given module named
`flow`, so we can eliminate the
`<dataflow_variable_name_or_factory_function>` part of our import
string.

Processes, workers, recovery stores and other options can be
configured with command line flags or environment variables. For the
full list of options see the `--help` command line flag:

``` bash
> python -m bytewax.run --help
usage: python -m bytewax.run [-h] [-p PROCESSES] [-w WORKERS_PER_PROCESS] [-i PROCESS_ID] [-a ADDRESSES] [--sqlite-directory SQLITE_DIRECTORY] [--epoch-interval EPOCH_INTERVAL] import_str

Run a bytewax dataflow

positional arguments:
  import_str            Dataflow import string in the format <module_name>:<dataflow_variable_or_factory> Example: src.dataflow:flow or src.dataflow:get_flow('string_argument')

options:
  -h, --help            show this help message and exit

Scaling:
  You should use either '-p' to spawn multiple processes on this same machine, or '-i/-a' to spawn a single process on different machines

  -p PROCESSES, --processes PROCESSES
                        Number of separate processes to run [env: BYTEWAX_PROCESSES]
  -w WORKERS_PER_PROCESS, --workers-per-process WORKERS_PER_PROCESS
                        Number of workers for each process [env: BYTEWAX_WORKERS_PER_PROCESS]
  -i PROCESS_ID, --process-id PROCESS_ID
                        Process id [env: BYTEWAX_PROCESS_ID]
  -a ADDRESSES, --addresses ADDRESSES
                        Addresses of other processes, separated by semicolumn: -a "localhost:2021;localhost:2022;localhost:2023" [env: BYTEWAX_ADDRESSES]

Recovery:
  --sqlite-directory SQLITE_DIRECTORY
                        Passing this argument enables sqlite recovery in the specified folder [env: BYTEWAX_SQLITE_DIRECTORY]
  --epoch-interval EPOCH_INTERVAL
                        Number of seconds between state snapshots [env: BYTEWAX_EPOCH_INTERVAL]
```

### Porting a simple example from 0.15 to 0.16

This is what a simple example looked like in `0.15`:

```python doctest:SKIP
import operator
import re

from datetime import timedelta, datetime

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import run_main
from bytewax.window import SystemClockConfig, TumblingWindowConfig


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for line in open("wordcount.txt"):
        yield state, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


clock_config = SystemClockConfig()
window_config = TumblingWindowConfig(length=timedelta(seconds=5))

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("sum", clock_config, window_config, add)
flow.capture(StdOutputConfig())

run_main(flow)
```

To port the example to the `0.16` version we need to make a few
changes.

#### Imports

Let's start with the existing imports:

```python doctest:SKIP
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import run_main
```

Becomes:
```python doctest:SKIP
from bytewax.connectors.files import FileInput
from bytewax.connectors.stdio import StdOutput
```

We removed `run_main` as it is now only used for unit testing
dataflows. Bytewax now has a built-in FileInput connector, which uses
the `PartitionedInput` connector superclass.

Since we are using a built-in connector to read from this file, we can
delete our `input_builder` function above.

#### Windowing

Most of the operators from `v0.15` are the same, but the `start_at`
parameter of windowing functions has been changed to `align_to`. The
`start_at` parameter incorrectly implied that there were no potential
windows before that time. What determines if an item is late for a
window is not the windowing definition, but the watermark of the
clock.

`SlidingWindow` and `TumblingWindow` now require the `align_to`
parameter. Previously, this was filled with the timestamp of the start
of the Dataflow, but must now be specified. Specifying this parameter
ensures that windows are consistent across Dataflow restarts, so make
sure that you don't change this parameter between executions.


The old `TumblingWindow` definition:

```python doctest:SKIP
clock_config = SystemClockConfig()
window_config = TumblingWindowConfig(length=timedelta(seconds=5))
```

becomes:

```python doctest:SKIP
clock_config = SystemClockConfig()
window_config = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)
```

#### Output and execution

Similarly to the `input`, the output configuration is now stateful.
`capture` has been renamed to `output`, and now takes a name, as all
stateful operators do.

So we move from this:

```python doctest:SKIP
flow.capture(StdOutputConfig())
```

To this:

```python doctest:SKIP
flow.output("out", StdOutput())
```

#### Complete code

The complete code for the new simple example now looks like this:

```python doctest:SKIP
import operator
import re

from datetime import timedelta, datetime, timezone

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileInput
from bytewax.connectors.stdio import StdOutput
from bytewax.window import SystemClockConfig, TumblingWindow


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


clock_config = SystemClockConfig()
window_config = TumblingWindow(
    length=timedelta(seconds=5), align_to=datetime(2023, 1, 1, tzinfo=timezone.utc)
)

flow = Dataflow()
flow.input("inp", FileInput("examples/sample_data/wordcount.txt"))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("sum", clock_config, window_config, add)
flow.output("out", StdOutput())
```

Running our completed Dataflow looks like this, as we've named our
Dataflow variable `flow` in a file named `dataflow`:

``` bash
> python -m bytewax.run dataflow:flow
('whether', 1)
("'tis", 1)
('of', 2)
('opposing', 1)
...
```

We can even run our sample Dataflow on multiple workers to process the
file in parallel:

``` bash
> python -m bytewax.run dataflow:flow -p2
('whether', 1)
("'tis", 1)
('of', 2)
('opposing', 1)
...
```

In the background, Bytewax has spawned two processes, each of which is
processing a part of the file. To see this more clearly, you can start
each worker by hand:

In one terminal, run:

``` bash
> python -m bytewax.run dataflow:flow -i0 -a "localhost:2101;localhost:2102"
('not', 1)
('end', 1)
('opposing', 1)
('take', 1)
...
```

And in a second terminal, run:

``` bash
> python -m bytewax.run dataflow:flow -i1 -a "localhost:2101;localhost:2102"
('question', 1)
('the', 3)
('troubles', 1)
('fortune', 1)
...
```

## From 0.10 to 0.11

Bytewax 0.11 introduces major changes to the way that Bytewax
dataflows are structured, as well as improvements to recovery and
windowing. This document outlines the major changes between Bytewax
0.10 and 0.11.

### Input and epochs

Bytewax is built on top of the [Timely
Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
framework. The idea of timestamps (which we refer to in Bytewax as
epochs) is central to Timely Dataflow's progress tracking mechanism.

Bytewax initially adopted an input model that included managing the
epochs at which input was introduced. The 0.11 version of Bytewax
removes the need to manage epochs directly.

Epochs continue to exist in Bytewax, but are now managed internally to
represent a unit of recovery. Bytewax dataflows that are configured
with recovery will shapshot their state after processing all items in
an epoch. In the event of recovery, Bytewax will resume a dataflow at
the last snapshotted state. The frequency of snapshotting can be
configured with an `EpochConfig`

### Recoverable input

Bytewax 0.11 will now allow you to recover the state of the input to
your dataflow.

Manually constructed input functions, like those used with
`ManualInputConfig` now take a third argument. If your dataflow is
interrupted, the third argument passed to your input function can be
used to reconstruct the state of your input at the last recovery
snapshot, provided you write your input logic accordingly. The
`input_builder` function must return a tuple of (resume_state, datum).

Bytewax's built-in input handlers, like `KafkaInputConfig` are also
recoverable. `KafkaInputConfig` will store information about consumer
offsets in the configured Bytewax recovery store. In the event of
recovery, `KafkaInputConfig` will start reading from the offsets that
were last committed to the recovery store.

### Stateful windowing

Version 0.11 also introduces stateful windowing operators, including a
new `fold_window` operator.

Previously, Bytewax included helper functions to manage windows in
terms of epochs. Now that Bytewax manages epochs internally, windowing
functions are now operators that appear as a processing step in a
dataflow. Dataflows can now have more than one windowing step.

Bytewax's stateful windowing operators are now built on top of its
recovery system, and their operations can be recovered in the event of
a failure. See the documentation on
<project:/articles/concepts/recovery.md> for more information.

### Output configurations

The 0.11 release of Bytewax adds some prepackaged output configuration
options for common use-cases:

`ManualOutputConfig` which calls a Python callback function for each
output item.

`StdOutputConfig` which prints each output item to stdout.

### Import path changes and removed entrypoints

In Bytewax 0.11, the overall Python module structure has changed, and
some execution entrypoints have been removed.

- `cluster_main`, `spawn_cluster`, and `run_main` have moved to
  `bytewax.execution`

- `Dataflow` has moved to `bytewax.dataflow`

- `run` and `run_cluster` have been removed

### Porting the Simple example from 0.10 to 0.11

This is what the `Simple example` looked like in `0.10`:

```python doctest:SKIP
import re

from bytewax import Dataflow, run


def file_input():
    for line in open("wordcount.txt"):
        yield 1, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


flow = Dataflow()
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_epoch(add)
flow.capture()


for epoch, item in run(flow, file_input()):
    print(item)
```

To port the example to the `0.11` version we need to make a few
changes.

#### Imports

Let's start with the existing imports:

```python doctest:SKIP
from bytewas import Dataflow, run
```

Becomes:
```python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
```

We moved from `run` to `run_main` as the execution API has been
simplified, and we can now just use the `run_main` function to execute
our dataflow.

### Input

The way bytewax handles input changed with `0.11`. `input` is now a
proper operator on the Dataflow, and the function now takes 3
parameters: `worker_index`, `worker_count`, `resume_state`. This
allows us to distribute the input across workers, and to handle
recovery if we want to. We are not going to do that in this example,
so the change is minimal.

The input function goes from:

```python doctest:SKIP
def file_input():
    for line in open("wordcount.txt"):
        yield 1, line
```

to:

```python doctest:SKIP
def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for line in open("wordcount.txt"):
        yield state, line
```

So instead of manually yielding the `epoch` in the input function, we
can either ignore it (passing `None` as state), or handle the value to
implement recovery (see our docs on
<project:/articles/concepts/recovery.md>).

Then we need to wrap the `input_builder` with `ManualInputConfig`,
give it a name ("file_input" here) and pass it to the `input` operator
(rather than the `run` function):

```python doctest:SKIP
from bytewax.inputs import ManualInputConfig


flow.input("file_input", ManualInputConfig(input_builder))
```

### Operators

Most of the operators are the same, but there is a notable change in
the flow: where we used `reduce_epoch` we are now using
`reduce_window`. Since the epochs concept is now considered an
internal detail in bytewax, we need to define a way to let the
`reduce` operator know when to close a specific window. Previously
this was done everytime the `epoch` changed, while now it can be
configured with a time window. We need two config objects to do this:

- `clock_config`

- `window_config`

The `clock_config` is used to tell the window-based operators what
reference clock to use, here we use the `SystemClockConfig` that just
uses the system's clock. The `window_config` is used to define the
time window we want to use. Here we'll use the `TumblingWindow` that
allows us to have tumbling windows defined by a length (`timedelta`),
and we configure it to have windows of 5 seconds each.

So the old `reduce_epoch`:
```python doctest:SKIP
flow.reduce_epoch(add)
```

becomes `reduce_window`:

```python doctest:SKIP
from bytewax.window import SystemClockConfig, TumblingWindow


clock_config = SystemClockConfig()
window_config = TumblingWindow(length=timedelta(seconds=5))
flow.reduce_window("sum", clock_config, window_config, add)
```

### Output and execution

Similarly to the `input`, the output configuration is now part of an
operator, `capture`. Rather than collecting the output in a python
iterator and then manually printing it, we can now configure the
`capture` operator to print to standard output.

Since all the input and output handling is now defined inside the
Dataflow, we don't need to pass this information to the execution
method.

So we move from this:

```python doctest:SKIP
flow.capture()

for epoch, item in run(flow, file_input()):
    print(item)
```

To this:

```python doctest:SKIP
from bytewax.outputs import StdOutputConfig


flow.capture(StdOutputConfig())

run_main(flow)
```

### Complete code

The complete code for the new simple example now looks like this:

```python doctest:SKIP
import operator
import re

from datetime import timedelta, datetime

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig
from bytewax.outputs import StdOutputConfig
from bytewax.execution import run_main
from bytewax.window import SystemClockConfig, TumblingWindow


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    for line in open("wordcount.txt"):
        yield state, line


def lower(line):
    return line.lower()


def tokenize(line):
    return re.findall(r'[^\s!,.?":;0-9]+', line)


def initial_count(word):
    return word, 1


def add(count1, count2):
    return count1 + count2


clock_config = SystemClockConfig()
window_config = TumblingWindow(length=timedelta(seconds=5))

flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))
flow.map(lower)
flow.flat_map(tokenize)
flow.map(initial_count)
flow.reduce_window("sum", clock_config, window_config, add)
flow.capture(StdOutputConfig())

run_main(flow)
```
