## Upgrading to v0.16

Bytewax v0.16 introduces major changes to the way that custom Bytewax inputs are created, as well as improvements to windowing and execution. In this article, we'll cover some of the updates we've made to our API to make upgrading to v0.16 easier.

## Input changes

In v0.16, we have restructured input to be based around partitions in order to support rescaling stateful dataflows in the future. We have also dropped the `Config` suffix to our input classes. As an example, `KafkaInputConfig` has been renamed to `KafkaInput` and has been moved to `bytewax.connectors.kafka.KafkaInput`.

`ManualInputConfig` is now replaced by two base superclasses that can be subclassed to create custom input sources. They are `DynamicInput` and `PartitionedInput`.

### `DynamicInput`

`DynamicInput` sources are input sources that support reading distinct items from any number of workers concurrently.

`DynamicInput` sources do not support resume state and thus generally do not provide delivery guarantees better than at-most-once.

### `PartitionedInput`

`PartitionedInput` sources can keep internal state on the current position of each partition. If a partition can "rewind" and resume reading from a previous position, they can provide delivery guarantees of at-least-once or better.

`PartitionedInput` sources maintain the state of each source and re-build that state during recovery.

## Deprecating Kafka Recovery

In order to better support rescaling Dataflows, the Kafka recovery store option is being deprecated and will be removed from a future release in favor of the SQLite recovery store.

## Capture -> Output

The `capture` operator has been renamed to `output` in v0.16 and is now stateful, so requires a step ID:

In v0.15 and before:

``` python doctest:SKIP
flow.capture(StdOutputConfig())
```

In v0.16+:

``` python
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.output("out", StdOutput())
```

`ManualOutputConfig` is now replaced by two base superclasses that can be subclassed to create custom output sinks. They are `DynamicOutput` and `PartitionedOutput`.

## New entrypoint

In Bytewax v0.16, the way that Dataflows are run has been simplified, and most execution functions have been removed.

Similar to other Python frameworks like Flask and FastAPI, Dataflows can now be run using a Datfaflow import string in the <module_name>:<dataflow_variable_name_or_factory_function> format. As an example, given a file named `dataflow.py` with the following contents:

``` python
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput
from bytewax.connectors.stdio import StdOutput

flow = Dataflow()
flow.input("in", TestingInput(range(3)))
flow.output("out", StdOutput())
```

Since a Python file `dataflow.py` defines a module `dataflow`, the Dataflow can be run with the following invocation:

``` bash
> python -m bytewax.run dataflow
0
1
2
```

By default, Bytewax looks for a variable in the given module named `flow`, so we can eliminate the `<dataflow_variable_name_or_factory_function>` part of our import string.

Processes, workers, recovery stores and other options can be configured with command line flags or environment variables. For the full list of options see the `--help` command line flag:

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

## Porting a simple example from 0.15 to 0.16

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

To port the example to the `0.16` version we need to make a few changes.

### Imports

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

We removed `run_main` as it is now only used for unit testing dataflows. Bytewax now has a built-in FileInput connector, which uses the `PartitionedInput` connector superclass.

Since we are using a built-in connector to read from this file, we can delete our `input_builder` function above.

### Windowing

Most of the operators from `v0.15` are the same, but the `start_at` parameter of windowing functions has been changed to `align_to`. The `start_at` parameter incorrectly implied that there were no potential windows before that time. What determines if an item is late for a window is not the windowing definition, but the watermark of the clock.

`SlidingWindow` and `TumblingWindow` now require the `align_to` parameter. Previously, this was filled with the timestamp of the start of the Dataflow, but must now be specified. Specifying this parameter ensures that windows are consistent across Dataflow restarts, so make sure that you don't change this parameter between executions.


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

### Output and execution

Similarly to the `input`, the output configuration is now stateful. `capture` has been renamed to `output`, and now takes a name, as all stateful operators do.

So we move from this:

```python doctest:SKIP
flow.capture(StdOutputConfig())
```

To this:

```python doctest:SKIP
flow.output("out", StdOutput())
```

### Complete code
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

Running our completed Dataflow looks like this, as we've named our Dataflow variable `flow` in a file named `dataflow`:

``` bash
python -m bytewax.run dataflow:flow
('whether', 1)
("'tis", 1)
('of', 2)
('opposing', 1)
...
```

We can even run our sample Dataflow on multiple workers to process the file in parallel:

``` bash
> python -m bytewax.run dataflow:flow -p2
('whether', 1)
("'tis", 1)
('of', 2)
('opposing', 1)
...
```

In the background, Bytewax has spawned two processes, each of which is processing a part of the file. To see this more clearly, you can start each worker by hand:

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
