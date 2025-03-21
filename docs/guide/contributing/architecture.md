# Architecture

## Tech Stack

Bytewax's is a dataflow programming library. It provides you with a
Python-based API to design arbitrary dataflow graphs, functional
transformations on streams of data, and connections to and from
external streaming data sources. The low-level dataflow runtime and
networking code is provided by the [Timely
Dataflow](https://github.com/TimelyDataflow/timely-dataflow) open
source project which is written in [Rust](https://www.rust-lang.org/).
Bytewax provides mid- and high-level APIs on top of Timely to allow
you to correctly and conveniently write fault-tolerant dataflows that
can connect with your custom data sources and sinks. Much of Bytewax's
code is written in Python, but some core components which interface
with Timely deeply are written in Rust and Python bindings are exposed
via [PyO3](https://pyo3.rs/). Local state persistence for recovery
functionality is provided by [SQLite](https://sqlite.org/). Internal
dataflow runtime metrics are gathered using the [Rust OpenTelemetry
SDK](https://opentelemetry.io/docs/languages/rust/) and available on a
small [Axum](https://github.com/tokio-rs/axum) web server.

![Bytewax architecture diagram.](/assets/arch-diagram.svg)

%https://excalidraw.com/#json=jrLaaNc51kqZARPRT62Ml,yOESiE7GCjE__eCg-Z8-bA

## Dataflow Structure

Bytewax dataflows are defined in terms of a set of
{py:obj}`dataclasses` which define the shape of the dataflow and use
closures and inheritance to package up all logic that the user wants
to perform once the dataflow is executing. These are defined in
{py:obj}`bytewax.dataflow`. Just instantiating the
{py:obj}`~bytewax.dataflow.Dataflow` or any of the operators does not
execute any dataflow code. You can read more about how to use operator
and chain together streams and the practical use of the dataflow
dataclasses in <project:#xref-dataflow-programming>. All dataflow
definition objects are pure Python.

Bytewax operators are composites of multiple sub-operators. Eventually
being based on a set of **core operators** which have no inner
structure and are implemented in the runtime. Each operator is defined
as a Python function and the {py:obj}`bytewax.dataflow.operator`
decorator uses dynamic inspection to look at the function definition
and produce the relevant inner-steps when called during dataflow
building. This is all orchestrated through `_gen_op_cls` and
`_gen_op_fn` in <gh-path:/pysrc/bytewax/dataflow.py>.

## Basic Execution

Once you have a dataflow definition, the dataflow can be actually
executed by the runtime. The user-facing entry point to this is by
calling `python -m bytewax.run`, which you can read about more at
<project:#xref-execution>, but that eventually calls into some PyO3
bridge code entry point functions defined in <gh-path:/src/run.rs>.
These call into Timely's setup functions to configure this process.

The core main run loop of each process is defined in `worker_main` in
<gh-path:/src/worker.rs>. This figures out how to resume from any
relevant recovery data, compiles the dataflow, and then delegates to
Timely's run-loop to actually execute the operators and run the
dataflow.

The **dataflow compilation** process is defined in
`build_production_dataflow` and is taking the instantiated
{py:obj}`~bytewax.dataflow.Dataflow` object defining the shape of the
dataflow in terms of core operators, and incrementally building out
equivalent Timely dataflow structures and streams. Because the
dataflow is a DAG, it's relatively straightforward to keep track of
back-references.

## Core Operators

Core operators are the operators that are actually implemented in Rust
using Timely. All other operators in the public API are composites
made up of a tree of core operators. The Rust Timely code for the core
operators is defined in <gh-path:/src/operators.rs>. Every Timely
operator is defined as a cooperative-multitasking task (although it
does not use any Rust `async` code); it needs to define what happens
when the runtime wakes up, have some reasonable amount of work to do,
and then yield control back to the runtime. needs to do a few things:

1. Setup input and output handles, metrics, working local state, etc.

2. Define it's key space and exchange pact. Do items for this step get
   routed to workers or just stay on the same worker?

3. Define the coroutine logic callback. This should call into the
   Python logic from the dataflow definition of the operator via PyO3.

The two main core operators are
{py:obj}`~bytewax.operators.flat_map_batch` and
{py:obj}`~bytewax.operators.stateful_flat_map`.
{py:obj}`~bytewax.operators.flat_map_batch` uses a simple callback as
the API. {py:obj}`~bytewax.operators.stateful_flat_map` uses an ABC
{py:obj}`~bytewax.operators.StatefulBatchLogic`. Both are called via
PyO3. Because it's currently not possible to extend a Python class
written in Rust via PyO3 via a pure-Python class,
{py:obj}`~bytewax.operators.StatefulBatchLogic` is pure-Python, but
then there's a shim code in a `struct StatefulBatchLogic` in
<gh-path:/src/operators.rs> to call each method in a way that feels
like an ABC.

## Recovery Backup

The logic in each operator needs to manage the epoch / timestamp
semantics of the stream. You can read more about [Timely's timestamps
in the Timely
Book](https://timelydataflow.github.io/timely-dataflow/chapter_1/chapter_1_2.html).
We call timestamps **epochs** as to disambiguate from the sense of
time in windows. Simplifying, Timely lets you "batch" items in a
stream in an epoch, then can let you know that no more items will
arrive in that epoch. That notification that the epoch has ended is
_cluster-wide_ and thus can be used as a distributed synchronization
mechanism. We use epochs for two separate features: backpressure and
coordinated recovery snapshots.

`StatefulBatchLogic` is the core operator which handles stateful
operators (that aren't IO). It makes a point to buffer incoming items
if they are "ahead" of the closed epoch. Then when an epoch closes,
the operator pauses and emits a {py:obj}`pickle`'d snapshot of the
state on a second output. These "epoch end" notifications, combined
with an item never changing epochs, allows for a wave of snapshots to
be taken through all stateful operators in the dataflow in a
consistent way.

All these state snapshots are routed into internal recovery operators
of the Timely dataflow; they are not visible in the user's dataflow.
This is defined in the `write_recovery` operator in
<gh-path:/src/recovery.rs>. This exchanges the state snapshots and
progress information into one of the SQLite recovery partitions on one
of the workers. Only once those snapshots have been written by all
workers is the epoch "over" and it can be written that the cluster can
resume from that new epoch.

You can read more about the user-facing recovery experience, backup
intervals, and the general design of recovery in
<project:#xref-recovery>.

## IO

Dataflow input and output are similar to normal stateful operators,
but need to be specialized code because:

- Inputs define the epochs.

- IO has special parallelization requirements and you might want to
  read from partitions independently on certain workers.

You can see the specialized Timely input and output operators in
<gh-path:/src/inputs.rs> and <gh-path:/src/outputs.rs>.

Epochs are defined by assuming that each worker's wall clock is
approximately synchronized, and some fixed interval (e.g. 10 seconds)
is used as the epoch interval. The input labels all items it's
emitting in that wall clock interval with the epoch integer, then
increments.

IO also has two modes of being instantiated: stateless /
`Dynamic{Source,Sink}` and stateful / `FixedPartitioned{Source,Sink}`.

In the stateless version, every worker runs the same IO code and reads
or writes whatever it needs to. In the stateful version, it models the
source or sink as having a fixed number of partitions with some state
for each partition. This can be used to re-scale the cluster in a
coherent way maintaining the state / position of the sources and
sinks.

Both the IO systems and the recovery snapshot writing system use some
shared code to properly distribute partition assignments across
workers in the cluster in <gh-path:/src/timely.rs>.

Similar to the stateful operators in the rest of the dataflow, the
main coroutine of the Timely operators delegates to the user logic
Python code via PyO3. {py:obj}`bytewax.inputs` and
{py:obj}`bytewax.outputs` contain ABCs which are the interface similar
to `StatefulBatchLogic`.

You can read more about the user-facing API of connectors at
<project:#xref-custom-connectors>.

## Resume

The core design of resuming a stateful dataflow is to rewind the state
to the last coherent snapshot across all workers, also rewind the
inputs and outputs to that point, and then start replaying data in the
dataflow. (Note that this means that no in-flight data between
operators needs to be persisted.)

`worker_main` in <gh-path:/src/worker.rs> has a part which
`build_resume_calc_dataflow` to determine which coordinated epoch
snapshot should be used. This uses the `ResumeFrom` operator in
<gh-path:/src/recovery.rs> to read from the available recovery
partitions and determine the last epoch which was written to every
partition in the last execution.

Then as part of the dataflow compiler `build_production_dataflow`,
some Timely operators are added to read all the relevant snapshots and
exchange them into a second input handle on every stateful operator.
Because the key used in this is the same as the routing key for actual
items flowing into the stateful operators, state is rendezvoused at
the correct worker, even in the case of a re-scale.

To read more about the user-facing experience of rescaling and some
principles see <project:#xref-rescaling>.
