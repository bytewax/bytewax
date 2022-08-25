## Changes introduced from 0.10 to 0.11

Bytewax 0.11 introduces major changes to the way that Bytewax dataflows are structured, as well as improvements to recovery and windowing. This document outlines the major changes between Bytewax 0.10 and 0.11.

## Input and epochs

Bytewax is built on top of the [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow) framework. The idea of timestamps (which we refer to in Bytewax as epochs) is central to Timely Dataflow's progress tracking mechanism.

Bytewax initially adopted an input model that included managing the epochs at which input was introduced. The 0.11 version of Bytewax removes the need to manage epochs directly.

Epochs continue to exist in Bytewax, but are now managed internally to represent a unit of recovery. Bytewax dataflows that are configured with [recovery](/apidocs#bytewax.recovery) will checkpoint their state after processing all items in an epoch. In the event of recovery, Bytewax will resume a dataflow at the last snapshotted state. The frequency of snapshotting can be configured with an [EpochConfig](/apidocs#bytewax.execution.EpochConfig).

## Recoverable input

Bytewax 0.11 will now allow you recover the state of the input to your dataflow.

Manually constructed input functions, like those used with [ManualInputConfig](/apidocs#bytewax.inputs.ManualInputConfig) now take a third argument. In the case that your dataflow is interrupted, the third argument passed to your input function can be used to reconstruct the state of your input at the last recovery snapshot. The input_builder function must return a tuple of (resume_state, datum).

Bytewax's built-in input handlers, like [KafkaInputConfig](/apidocs#bytewax.inputs.KafkaInputConfig) are also recoverable. `KafkaInputConfig` will store information about consumer offsets in the configured Bytewax recovery store. In the event of recovery, `KafkaInputConfig` will start reading from the offsets that were last committed to the recovery store.

## Stateful windowing

Version 0.11 also introduces stateful windowing operators, including a new [fold_window](/apidocs#bytewax.Dataflow.fold_window) operator.

Previously, Bytewax included helper functions to manage windows in terms of epochs. Now that Bytewax manages epochs internally, windowing functions are now operators that appear as a processing step in a dataflow. Dataflows can now have more than one windowing step.

Bytewax's stateful windowing operators are now built on top of its recovery system, and be recovered in the event of a failure. See the documentation on [recovery](/apidocs#bytewax.recovery) for more information.

## Output configurations

The 0.11 release of Bytewax adds some prepackaged output configuration options for common use-cases:

[ManualOutputConfig](/apidocs#bytewax.outputs.ManualOutputConfig), which calls a Python callback function for each output item.

[StdOutputConfig](/apidocs#bytewax.outputs.StdOutputConfig), which prints each output item to stdout.

## Import path changes and removed entrypoints

In Bytewax 0.11, The overall Python module structure has changed, and some execution entrypoints have been removed.

- `cluster_main`, `spawn_cluster` and `run_main` have moved to `bytewax.execution`
- `Dataflow` has moved to `bytewax.dataflow`
- `run` and `run_cluster` have been removed
