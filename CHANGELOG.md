# Bytewax Changelog

All notable changes to this project will be documented in this file.
For help with updating to new Bytewax versions, please see the
[migration guide](https://bytewax.io/docs/reference/migration).

## Latest

__Add any extra change notes here and we'll put them in the release
notes on GitHub when we make a new release.__

- *Breaking change* The schema registry interface has been removed.
  You can still use schema registries, but you need to instantiate
  the (de)serializers on your own. This allows for more flexibility.
  See the `confluent_serde` and `redpanda_serde` examples for how
  to use the new interface.

- Fixes bug where items would be incorrectly marked as late in sliding
  and tumbling windows in cases where the timestamps are very far from
  the `align_to` parameter of the windower.

- Adds `stateful_flat_map` operator.

- *Breaking change* Removes `builder` argument from `stateful_map`.
  Instead, the initial state value is always `None` and you can call
  your previous builder by hand in the `mapper`.

- *Breaking change* Improves performance by removing the `now:
  datetime` argument from `FixedPartitionedSource.build_part`,
  `DynamicSource.build`, and `UnaryLogic.on_item`. If you need the
  current time, use:

```python
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
```

- *Breaking change* Improves performance by removing the `sched:
  datetime` argument from `StatefulSourcePartition.next_batch`,
  `StatelessSourcePartition.next_batch`, `UnaryLogic.on_notify`. You
  should already have the scheduled next awake time in whatever
  instance variable you returned in
  `{Stateful,Stateless}SourcePartition.next_awake` or
  `UnaryLogic.notify_at`.

## v0.18.2

- Fixes a bug that prevented the deletion of old state in recovery stores.

- Better error messages on invalid epoch and backup interval
  parameters.

- Fixes bug where dataflow will hang if a source's `next_awake` is set
  far in the future.

## v0.18.1

- Changes the default batch size for `KafkaSource` from 1 to 1000 to match
  the Kafka input operator.

- Fixes an issue with the `count_window` operator:
  https://github.com/bytewax/bytewax/issues/364.

## v0.18.0

- Support for schema registries, through
  `bytewax.connectors.kafka.registry.RedpandaSchemaRegistry` and
  `bytewax.connectors.kafka.registry.ConfluentSchemaRegistry`.

- Custom Kafka operators in `bytewax.connectors.kafka.operators`:
  `input`, `output`, `deserialize_key`, `deserialize_value`,
  `deserialize`, `serialize_key`, `serialize_value` and `serialize`.

- *Breaking change* `KafkaSource` now emits a special
  `KafkaSourceMessage` to allow access to all data on consumed
  messages. `KafkaSink` now consumes `KafkaSinkMessage` to allow
  setting additional fields on produced messages.

- Non-linear dataflows are now possible. Each operator method returns
  a handle to the `Stream`s it produces; add further steps via calling
  operator functions on those returned handles, not the root
  `Dataflow`. See the migration guide for more info.

- Auto-complete and type hinting on operators, inputs, outputs,
  streams, and logic functions now works.

- A ton of new operators: `collect_final`, `count_final`,
  `count_window`, `flatten`, `inspect_debug`, `join`, `join_named`,
  `max_final`, `max_window`, `merge`, `min_final`, `min_window`,
  `key_on`, `key_assert`, `key_split`, `merge`, `unary`. Documentation
  for all operators are in `bytewax.operators` now.

- New operators can be added in Python, made by grouping existing
  operators. See `bytewax.dataflow` module docstring for more info.

- *Breaking change* Operators are now stand-alone functions; `import
  bytewax.operators as op` and use e.g. `op.map("step_id", upstream,
  lambda x: x + 1)`.

- *Breaking change* All operators must take a `step_id` argument now.

- *Breaking change* `fold` and `reduce` operators have been renamed to
  `fold_final` and `reduce_final`. They now only emit on EOF and are
  only for use in batch contexts.

- *Breaking change* `batch` operator renamed to `collect`, so as to
  not be confused with runtime batching. Behavior is unchanged.

- *Breaking change* `output` operator does not forward downstream its
  items. Add operators on the upstream handle instead.

- `next_batch` on input partitions can now return any `Iterable`, not
  just a `List`.

- `inspect` operator now has a default inspector that prints out items
  with the step ID.

- `collect_window` operator now can collect into `set`s and `dict`s.

- Adds a `get_fs_id` argument to `{Dir,File}Source` to allow handling
  non-identical files per worker.

- Adds a `TestingSource.EOF` and `TestingSource.ABORT` sentinel values
  you can use to test recovery.

- *Breaking change* Adds a `datetime` argument to
  `FixedPartitionSource.build_part`, `DynamicSource.build_part`,
  `StatefulSourcePartition.next_batch`, and
  `StatelessSourcePartition.next_batch`. You can now use this to
  update your `next_awake` time easily.

- *Breaking change* Window operators now emit `WindowMetadata` objects
  downstream. These objects can be used to introspect the open_time
  and close_time of windows. This changes the output type of windowing
  operators from: `(key, values)` to `(key, (metadata, values))`.

- *Breaking change* IO classes and connectors have been renamed to
  better reflect their semantics and match up with documentation.

- Moves the ability to start multiple Python processes with the
  `-p` or `--processes` to the `bytewax.testing` module.

- *Breaking change* `SimplePollingSource` moved from
  `bytewax.connectors.periodic` to `bytewax.inputs` since it is an
  input helper.

- `SimplePollingSource`'s `align_to` argument now works.

## v0.17.1

- Adds the `batch` operator to Dataflows. Calling `Dataflow.batch`
  will batch incoming items until either a batch size has been reached
  or a timeout has passed.

- Adds the `SimplePollingInput` source. Subclass this input source to
  periodically source new input for a dataflow.

- Re-adds GLIBC 2.27 builds to support older linux distributions.

## v0.17.0

### Changed

- *Breaking change* Recovery system re-worked. Kafka-based recovery
  removed. SQLite recovery file format changed; existing recovery DB
  files can not be used. See the module docstring for
  `bytewax.recovery` for how to use the new recovery system.

- Dataflow execution supports rescaling over resumes. You can now
  change the number of workers and still get proper execution and
  recovery.

- `epoch-interval` has been renamed to `snapshot-interval`

- The `list-parts` method of `PartitionedInput` has been changed to
  return a `List[str]` and should only reflect the available
  inputs that a given worker has access to. You no longer need
  to return the complete set of partitions for all workers.

- The `next` method of `StatefulSource` and `StatelessSource` has
  been changed to `next_batch` and should return a `List` of elements,
  or the empty list if there are no elements to return.

### Added

- Added new cli parameter `backup-interval`, to configure the length of
  time to wait before "garbage collecting" older recovery snapshots.

- Added `next_awake` to input classes, which can be used to schedule
  when the next call to `next_batch` should occur. Use `next_awake`
  instead of `time.sleep`.

- Added `bytewax.inputs.batcher_async` to bridge async Python libraries
  in Bytewax input sources.

- Added support for linux/aarch64 and linux/armv7 platforms.

### Removed

- `KafkaRecoveryConfig` has been removed as a recovery store.

## v0.16.2

- Add support for Windows builds - thanks @zzl221000!
- Adds a CSVInput subclass of FileInput

## v0.16.1

- Add a cooldown for activating workers to reduce CPU consumption.
- Add support for Python 3.11.

## v0.16.0

- *Breaking change* Reworked the execution model. `run_main` and `cluster_main`
  have been moved to `bytewax.testing` as they are only supposed to be used
  when testing or prototyping.
  Production dataflows should be ran by calling the `bytewax.run`
  module with `python -m bytewax.run <dataflow-path>:<dataflow-name>`.
  See `python -m bytewax.run -h` for all the possible options.
  The functionality offered by `spawn_cluster` are now only offered by the
  `bytewax.run` script, so `spawn_cluster` was removed.

- *Breaking change* `{Sliding,Tumbling}Window.start_at` has been
  renamed to `align_to` and both now require that argument. It's not
  possible to recover windowing operators without it.

- Fixes bugs with windows not closing properly.

- Fixes an issue with SQLite-based recovery. Previously you'd always
  get an "interleaved executions" panic whenever you resumed a cluster
  after the first time.

- Add `SessionWindow` for windowing operators.

- Add `SlidingWindow` for windowing operators.

- *Breaking change* Rename `TumblingWindowConfig` to `TumblingWindow`

- Add `filter_map` operator.

- *Breaking change* New partition-based input and output API. This
  removes `ManualInputConfig` and `ManualOutputConfig`. See
  `bytewax.inputs` and `bytewax.outputs` for more info.

- *Breaking change* `Dataflow.capture` operator is renamed to
  `Dataflow.output`.

- *Breaking change* `KafkaInputConfig` and `KafkaOutputConfig` have
  been moved to `bytewax.connectors.kafka.KafkaInput` and
  `bytewax.connectors.kafka.KafkaOutput`.

- *Deprecation warning* The `KafkaRecovery` store is being deprecated
  in favor of `SqliteRecoveryConfig`, and will be removed in a future
  release.

## 0.15.0

- *Breaking change* Fixes issue with multi-worker recovery. If the
  cluster crashed before all workers had completed their first epoch,
  the cluster would resume from the incorrect position. This requires
  a change to the recovery store. You cannot resume from recovery data
  written with an older version.

## 0.14.0

- Dataflow continuation now works. If you run a dataflow over a finite
  input, all state will be persisted via recovery so if you re-run the
  same dataflow pointing at the same input, but with more data
  appended at the end, it will correctly continue processing from the
  previous end-of-stream.

- Fixes issue with multi-worker recovery. Previously resume data was
  being routed to the wrong worker so state would be missing.

- *Breaking change* The above two changes require that the recovery
  format has been changed for all recovery stores. You cannot resume
  from recovery data written with an older version.

- Adds an introspection web server to dataflow workers.

- Adds `collect_window` operator.

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
