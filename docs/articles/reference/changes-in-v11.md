## Changes introduced from 0.10 to 0.11

Bytewax 0.11 introduces major changes to the way that Bytewax dataflows are structured, as well as improvements to recovery and windowing. This document outlines the major changes between Bytewax 0.10 and 0.11.

## Input and epochs

Bytewax is built on top of the [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow) framework. The idea of timestamps (which we refer to in Bytewax as epochs) is central to Timely Dataflow's progress tracking mechanism.

Bytewax initially adopted an input model that included managing the epochs at which input was introduced. The 0.11 version of Bytewax removes the need to manage epochs directly.

Epochs continue to exist in Bytewax, but are now managed internally to represent a unit of recovery. Bytewax dataflows that are configured with [recovery](/apidocs/bytewax.recovery) will shapshot their state after processing all items in an epoch. In the event of recovery, Bytewax will resume a dataflow at the last snapshotted state. The frequency of snapshotting can be configured with an [EpochConfig](/apidocs/bytewax.execution#bytewax.execution.EpochConfig).

## Recoverable input

Bytewax 0.11 will now allow you to recover the state of the input to your dataflow.

Manually constructed input functions, like those used with [ManualInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.ManualInputConfig), now take a third argument. If your dataflow is interrupted, the third argument passed to your input function can be used to reconstruct the state of your input at the last recovery snapshot, provided you write your input logic accordingly. The `input_builder` function must return a tuple of (resume_state, datum).

Bytewax's built-in input handlers, like [KafkaInputConfig](/apidocs/bytewax.inputs#bytewax.inputs.KafkaInputConfig) are also recoverable. `KafkaInputConfig` will store information about consumer offsets in the configured Bytewax recovery store. In the event of recovery, `KafkaInputConfig` will start reading from the offsets that were last committed to the recovery store.

## Stateful windowing

Version 0.11 also introduces stateful windowing operators, including a new [fold_window](/apidocs/bytewax.dataflow#bytewax.Dataflow.fold_window) operator.

Previously, Bytewax included helper functions to manage windows in terms of epochs. Now that Bytewax manages epochs internally, windowing functions are now operators that appear as a processing step in a dataflow. Dataflows can now have more than one windowing step.

Bytewax's stateful windowing operators are now built on top of its recovery system, and their operations can be recovered in the event of a failure. See the documentation on [recovery](/apidocs/bytewax.recovery) for more information.

## Output configurations

The 0.11 release of Bytewax adds some prepackaged output configuration options for common use-cases:

[ManualOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.ManualOutputConfig), which calls a Python callback function for each output item.

[StdOutputConfig](/apidocs/bytewax.outputs#bytewax.outputs.StdOutputConfig), which prints each output item to stdout.

## Import path changes and removed entrypoints

In Bytewax 0.11, the overall Python module structure has changed, and some execution entrypoints have been removed.

- `cluster_main`, `spawn_cluster`, and `run_main` have moved to `bytewax.execution`
- `Dataflow` has moved to `bytewax.dataflow`
- `run` and `run_cluster` have been removed

## Porting the Simple example from 0.10 to 0.11

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

To port the example to the `0.11` version we need to make a few changes.

### Imports
Let's start with the existing imports:

```python doctest:SKIP
from bytewas import Dataflow, run
```

Becomes:
```python doctest:SKIP
from bytewax.dataflow import Dataflow
from bytewax.execution import run_main
```

We moved from `run` to `run_main` as the execution API has been simplified, and we can now just use the `run_main` function to execute our dataflow.

### Input

The way bytewax handles input changed with `0.11`. `input` is now a proper operator on the Dataflow, and the function now takes 3 parameters: `worker_index`, `worker_count`, `resume_state`.
This allows us to distribute the input across workers, and to handle recovery if we want to.
We are not going to do that in this example, so the change is minimal.

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

So instead of manually yielding the `epoch` in the input function, we can either ignore it (passing `None` as state), or handle the value to implement recovery (see the [recovery chapter](/docs/getting-started/recovery)).

Then we need to wrap the `input_builder` with `ManualInputConfig`, give it a name ("file_input" here) and pass it to the `input` operator (rather than the `run` function):

```python doctest:SKIP
from bytewax.inputs import ManualInputConfig


flow.input("file_input", ManualInputConfig(input_builder))
```

### Operators

Most of the operators are the same, but there is a notable change in the flow: where we used `reduce_epoch` we are now using `reduce_window`.
Since the epochs concept is now considered an internal detail in bytewax, we need to define a way to let the `reduce` operator know when to close a specific window.
Previously this was done everytime the `epoch` changed, while now it can be configured with a time window.
We need two config objects to do this:
- `clock_config`
- `window_config`

The `clock_config` is used to tell the window-based operators what reference clock to use, here we use the `SystemClockConfig` that just uses the system's clock.
The `window_config` is used to define the time window we want to use. Here we'll use the `TumblingWindow` that allows us to have tumbling windows defined by a length (`timedelta`), and we configure it to have windows of 5 seconds each.

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

Similarly to the `input`, the output configuration is now part of an operator, `capture`.
Rather than collecting the output in a python iterator and then manually printing it, we can now configure the `capture` operator to print to standard output.

Since all the input and output handling is now defined inside the Dataflow, we don't need to pass this information to the execution method.

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
