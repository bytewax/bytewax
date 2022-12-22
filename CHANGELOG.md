# Bytewax Changelog

## Latest

__Add any extra change notes here and we'll put them in the release
notes on GitHub when we make a new release.__

## 0.13.2

- Dataflow continuation now works. If you run a dataflow over a finite
  input, all state will be persisted via recovery so if you re-run the
  same dataflow pointing at the same input, but with more data
  appended at the end, it will correctly continue processing from the
  previous end-of-stream.

- Fixes issue with multi-worker recovery. Previously resume data was
  being routed to the wrong worker so state would be missing.

- Adds an introspection web server to dataflow workers.

## 0.13.1

- Added Google Colab support.

## 0.13.0

- Added tracing instrumentation and configurations for tracing backends.

## 0.12.0

- Fixes bug where window is never closed if recovery occurs after last
  item but before window close.

- Recovery logging is reduced.

- *Breaking change* Recovery format has been changed for all recovery stores.
  You cannot resume from recovery data written with an older version.

- Adds a `DynamoDB` and `Bigquery` output connector.

## 0.11.2

- Performance improvements.

- Support SASL and SSL for `bytewax.inputs.KafkaInputConfig`.


## 0.11.1

- KafkaInputConfig now accepts additional properties. See
  `bytewax.inputs.KafkaInputConfig`.

- Support for a pre-built Kafka output component. See
  `bytewax.outputs.KafkaOutputConfig`.

## 0.11.0

- Added the `fold_window` operator, works like `reduce_window` but allows
  the user to build the initial accumulator for each key in a `builder` function.

- Output is no longer specified using an `output_builder` for the
  entire dataflow, but you supply an "output config" per capture. See
  `bytewax.outputs` for more info.

- Input is no longer specified on the execution entry point (like
  `run_main`), it is instead using the `Dataflow.input` operator.

- Epochs are no longer user-facing as part of the input system. Any
  custom Python-based input components you write just need to be
  iterators and emit items. Recovery snapshots and backups now happen
  periodically, defaulting to every 10 seconds.

- Recovery format has been changed for all recovery stores. You cannot
  resume from recovery data written with an older version.

- The `reduce_epoch` operator has been replaced with
  `reduce_window`. It takes a "clock" and a "windower" to define the
  kind of aggregation you want to do.

- `run` and `run_cluster` have been removed and the remaining
  execution entry points moved into `bytewax.execution`. You can now
  get similar prototyping functionality with
  `bytewax.execution.run_main` and `bytewax.execution.spawn_cluster`
  using `Testing{Input,Output}Config`s.

- `Dataflow` has been moved into `bytewax.dataflow.Dataflow`.

## 0.10.0

- Input is no longer specified using an `input_builder`, but now an
  `input_config` which allows you to use pre-built input
  components. See `bytewax.inputs` for more info.

- Preliminary support for a pre-built Kafka input component. See
  `bytewax.inputs.KafkaInputConfig`.

- Keys used in the `(key, value)` 2-tuples to route data for stateful
  operators (like `stateful_map` and `reduce_epoch`) must now be
  strings. Because of this `bytewax.exhash` is no longer necessary and
  has been removed.

- Recovery format has been changed for all recovery stores. You cannot
  resume from recovery data written with an older version.

- Slight changes to `bytewax.recovery.RecoveryConfig` config options
  due to recovery system changes.

- `bytewax.run()` and `bytewax.run_cluster()` no longer take
  `recovery_config` as they don't support recovery.


## 0.9.0

- Adds `bytewax.AdvanceTo` and `bytewax.Emit` to control when processing
  happens.

- Adds `bytewax.run_main()` as a way to test input and output builders
  without starting a cluster.

- Adds a `bytewax.testing` module with helpers for testing.

- `bytewax.run_cluster()` and `bytewax.spawn_cluster()` now take a
  `mp_ctx` argument to allow you to change the multiprocessing
  behavior. E.g. from "fork" to "spawn". Defaults now to "spawn".

- Adds dataflow recovery capabilities. See `bytewax.recovery`.

- Stateful operators `bytewax.Dataflow.reduce()` and
  `bytewax.Dataflow.stateful_map()` now require a `step_id` argument
  to handle recovery.

- Execution entry points now take configuration arguments as kwargs.

## 0.8.0

- Capture operator no longer takes arguments. Items that flow through
  those points in the dataflow graph will be processed by the output
  handlers setup by each execution entry point. Every dataflow
  requires at least one capture.

- `Executor.build_and_run()` is replaced with four entry points for
  specific use cases:

  - `run()` for exeuction in the current process. It returns all
    captured items to the calling process for you. Use this for
    prototyping in notebooks and basic tests.

  - `run_cluster()` for execution on a temporary machine-local cluster
    that Bytewax coordinates for you. It returns all captured items to
    the calling process for you. Use this for notebook analysis where
    you need parallelism.

  - `spawn_cluster()` for starting a machine-local cluster with more
    control over input and output. Use this for standalone scripts
    where you might need partitioned input and output.

  - `cluster_main()` for starting a process that will participate in a
    cluster you are coordinating manually. Use this when starting a
    Kubernetes cluster.

- Adds `bytewax.parse` module to help with reading command line
  arguments and environment variables for the above entrypoints.

- Renames `bytewax.inp` to `bytewax.inputs`.
