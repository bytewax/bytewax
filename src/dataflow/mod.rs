//! Internal definition of Bytewax dataflows.
//!
//! For a user-centric version of how to define a dataflow, read the
//! `bytewax.dataflow` Python module docstring. Read that first.
//!
//! This is a "blueprint" of a dataflow. We compile this into a Timely
//! dataflow in [`crate::execution`] in the (private) `build_production_dataflow`
//! function.
//! We can't call into this structure directly from Timely because PyO3 does
//! not like generics.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::inputs::Input;
use crate::outputs::Output;
use crate::pyo3_extensions::TdPyCallable;
use crate::recovery::StepId;
use crate::window::clock::ClockConfig;
use crate::window::WindowConfig;

/// A definition of a Bytewax dataflow graph.
///
/// Use the methods defined on this class to add steps with operators
/// of the same name.
// TODO: Right now this is just a linear dataflow only.
#[pyclass(module = "bytewax.dataflow")]
pub(crate) struct Dataflow {
    pub(crate) steps: Vec<Step>,
}

#[pymethods]
impl Dataflow {
    #[new]
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// At least one input is required on every dataflow.
    ///
    /// Emits items downstream from the input source.
    ///
    /// See `bytewax.inputs` for more information on how input works.
    /// See `bytewax.connectors` for a buffet of our built-in
    /// connector types.
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   input (bytewax.inputs.Input):
    ///       Input definition.
    fn input(&mut self, step_id: StepId, input: Input) {
        self.steps.push(Step::Input { step_id, input });
    }

    /// Redistribute items randomly across all workers for the next step.
    ///
    /// Bytewax's execution model has workers executing all steps, but
    /// the state in each step is partitioned across workers by some
    /// key. Bytewax will only exchange an item between workers before
    /// stateful steps in order to ensure correctness, that they
    /// interact with the correct state for that key. Stateless
    /// operators (like `filter`) are run on all workers and do not
    /// result in exchanging items before or after they are run.
    ///
    /// This can result in certain ordering of operators to result in
    /// poor parallelization across an entire execution cluster. If
    /// the previous step (like a `reduce_window` or `input` with a
    /// `PartitionedInput`) concentrated items on a subset of workers
    /// in the cluster, but the next step is a CPU-intensive stateless
    /// step (like a `map`), it's possible that not all workers will
    /// contribute to processing the CPU-intesive step.
    ///
    /// This operation has a overhead, since it will need
    /// to serialize, send, and deserialize the items,
    /// so while it can significantly speed up the
    /// execution in some cases, it can also make it slower.
    ///
    /// A good use of this operator is to parallelize an IO
    /// bound step, like a network request, or a heavy,
    /// single-cpu workload, on a machine with multiple
    /// workers and multiple cpu cores that would remain
    /// unused otherwise.
    ///
    /// A bad use of this operator is if the operation you want
    /// to parallelize is already really fast as it is, as the
    /// overhead can overshadow the advantages of distributing
    /// the work.
    /// Another case where you could see regressions in performance is
    /// if the heavy CPU workload already spawns enough threads
    /// to use all the available cores. In this case multiple
    /// processes trying to compete for the cpu can end up being
    /// slower than doing the work serially.
    /// If the workers run on different machines though, it might
    /// again be a valuable use of the operator.
    ///
    /// Use this operator with caution, and measure whether you
    /// get an improvement out of it.
    ///
    /// Once the work has been spread to another worker, it will
    /// stay on those workers unless other operators explicitely
    /// move the item again (usually on output).
    fn redistribute(&mut self) {
        self.steps.push(Step::Redistribute);
    }

    /// Write data to an output.
    ///
    /// At least one output is required on every dataflow.
    ///
    /// Emits items downstream unmodified.
    ///
    /// See `bytewax.outputs` for more information on how output
    /// works. See `bytewax.connectors` for a buffet of our built-in
    /// connector types.
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   output (bytewax.outputs.Output):
    ///       Output definition.
    ///   min_batch_size (int):
    ///       minimum number of messages to receive before writing
    ///       to the output (default = 1)
    ///   timeout (Optional[timedelta]):
    ///       timeout after which items are emitted even if
    ///       `min_batch_size` is not reached.
    ///       If this is not set, items won't be emitted until
    ///       `min_batch_size` is reached.
    #[pyo3(signature = (step_id, output, min_batch_size = 1, timeout = None))]
    fn output(
        &mut self,
        step_id: StepId,
        output: Output,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
    ) {
        self.steps.push(Step::Output {
            step_id,
            output,
            min_batch_size,
            timeout,
        });
    }

    /// Filter selectively keeps only some items.
    ///
    /// It calls a **predicate** function on each item.
    ///
    /// It emits the item downstream unmodified if the predicate
    /// returns `True`.
    ///
    /// It is commonly used for:
    ///
    /// - Selecting relevant events
    /// - Removing empty events
    /// - Removing sentinels
    /// - Removing stop words
    ///
    /// >>> from bytewax.testing import TestingInput
    /// >>> from bytewax.connectors.stdio import StdOutput
    /// >>> from bytewax.testing import run_main
    /// >>> from bytewax.dataflow import Dataflow
    /// >>>
    /// >>> flow = Dataflow()
    /// >>> flow.input("inp", TestingInput(range(4)))
    /// >>> def is_odd(item):
    /// ...     return item % 2 != 0
    /// >>> flow.filter(is_odd)
    /// >>> flow.output("out", StdOutput())
    /// >>> run_main(flow)
    /// 1
    /// 3
    ///
    /// Args:
    ///   predicate:
    ///       `predicate(item: Any) => should_emit: bool`
    fn filter(&mut self, predicate: TdPyCallable) {
        self.steps.push(Step::Filter { predicate });
    }

    /// Filter map acts as a normal map function,
    /// but if the mapper returns None, the item
    /// is filtered out.
    ///
    /// >>> flow = Dataflow()
    /// >>> def validate(data):
    /// ...     if type(data) != dict or "key" not in data:
    /// ...         return None
    /// ...     else:
    /// ...         return data["key"], data
    /// ...
    /// >>> flow.filter_map(validate)
    ///
    /// Args:
    ///     mapper:
    ///         `mapper(item: Any) => modified_item: Optional[Any]`
    fn filter_map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::FilterMap { mapper });
    }

    /// Flat map is a one-to-many transformation of items.
    ///
    /// It calls a **mapper** function on each item.
    ///
    /// It emits each element in the returned iterator individually
    /// downstream in the epoch of the input item.
    ///
    /// It is commonly used for:
    ///
    /// - Tokenizing
    /// - Flattening hierarchical objects
    /// - Breaking up aggregations for further processing
    ///
    /// >>> from bytewax.testing import TestingInput
    /// >>> from bytewax.connectors.stdio import StdOutput
    /// >>> from bytewax.testing import run_main
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> flow = Dataflow()
    /// >>> inp = ["hello world"]
    /// >>> flow.input("inp", TestingInput(inp))
    /// >>> def split_into_words(sentence):
    /// ...     return sentence.split()
    /// >>> flow.flat_map(split_into_words)
    /// >>> flow.output("out", StdOutput())
    /// >>> run_main(flow)
    /// hello
    /// world
    ///
    /// Args:
    ///   mapper:
    ///       `mapper(item: Any) => emit: Iterable[Any]`
    fn flat_map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::FlatMap { mapper });
    }

    /// Inspect allows you to observe, but not modify, items.
    ///
    /// It calls an **inspector** callback on each item.
    ///
    /// It emits items downstream unmodified.
    ///
    /// It is commonly used for debugging.
    ///
    /// >>> from bytewax.testing import TestingInput, TestingOutput
    /// >>> from bytewax.testing import run_main
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> flow = Dataflow()
    /// >>> flow.input("inp", TestingInput(range(3)))
    /// >>> def log(item):
    /// ...     print("Saw", item)
    /// >>> flow.inspect(log)
    /// >>> out = []
    /// >>> flow.output("out", TestingOutput(out))  # Notice we don't print out.
    /// >>> run_main(flow)
    /// Saw 0
    /// Saw 1
    /// Saw 2
    ///
    /// Args:
    ///   inspector:
    ///       `inspector(item: Any) => None`
    fn inspect(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::Inspect { inspector });
    }

    /// Inspect epoch allows you to observe, but not modify, items and
    /// their epochs.
    ///
    /// It calls an **inspector** function on each item with its
    /// epoch.
    ///
    /// It emits items downstream unmodified.
    ///
    /// It is commonly used for debugging.
    ///
    /// >>> from datetime import timedelta
    /// >>> from bytewax.testing import TestingInput, TestingOutput, run_main
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> flow = Dataflow()
    /// >>> flow.input("inp", TestingInput(range(3)))
    /// >>> def log(epoch, item):
    /// ...    print(f"Saw {item} @ {epoch}")
    /// >>> flow.inspect_epoch(log)
    /// >>> out = []
    /// >>> flow.output("out", TestingOutput(out))  # Notice we don't print out.
    /// >>> run_main(flow, epoch_interval=timedelta(seconds=0))
    /// Saw 0 @ 1
    /// Saw 1 @ 2
    /// Saw 2 @ 3
    ///
    /// Args:
    ///   inspector:
    ///       `inspector(epoch: int, item: Any) => None`
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
    }

    /// Inspect worker allows you to observe, but not modify, items and
    /// their worker's index.
    ///
    /// It calls an **inspector** function on each item with its
    /// worker's index.
    ///
    /// It emits items downstream unmodified.
    ///
    /// It is commonly used for debugging.
    ///
    /// Args:
    ///   inspector:
    ///       `inspector(item: Any, worker: int) => None`
    fn inspect_worker(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectWorker { inspector });
    }

    /// Map is a one-to-one transformation of items.
    ///
    /// It calls a **mapper** function on each item.
    ///
    /// It emits each updated item downstream.
    ///
    /// It is commonly used for:
    ///
    /// - Extracting keys
    /// - Turning JSON into objects
    /// - So many things
    ///
    /// >>> from bytewax.connectors.stdio import StdOutput
    /// >>> from bytewax.testing import run_main, TestingInput
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> flow = Dataflow()
    /// >>> flow.input("inp", TestingInput(range(3)))
    /// >>> def add_one(item):
    /// ...     return item + 10
    /// >>> flow.map(add_one)
    /// >>> flow.output("out", StdOutput())
    /// >>> run_main(flow)
    /// 10
    /// 11
    /// 12
    ///
    /// Args:
    ///   mapper:
    ///       `mapper(item: Any) => updated_item: Any`
    fn map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::Map { mapper });
    }

    /// Reduce lets you combine items for a key into an accumulator.
    ///
    /// It is a stateful operator. It requires the input stream
    /// has items that are `(key: str, value)` tuples so we can ensure
    /// that all relevant values are routed to the relevant state. It
    /// also requires a step ID to recover the correct state.
    ///
    /// It calls two functions:
    ///
    /// - A **reducer** which combines a new value with an
    /// accumulator. The accumulator is initially the first value seen
    /// for a key. Values will be passed in an arbitrary order. If
    /// there is only a single value for a key since the last
    /// completion, this function will not be called.
    ///
    /// - An **is complete** function which returns `True` if the most
    /// recent `(key, accumulator)` should be emitted downstream and
    /// the accumulator for that key forgotten. If there was only a
    /// single value for a key, it is passed in as the accumulator
    /// here.
    ///
    /// It emits `(key, accumulator)` tuples downstream when the is
    /// complete function returns `True` in the epoch of the most
    /// recent value for that key.
    ///
    /// If the ordering of values is crucial, group beforhand using a
    /// windowing operator with a timeout like `reduce_window`, then
    /// sort, then use this operator.
    ///
    /// It is commonly used for:
    ///
    /// - Collection into a list
    /// - Summarizing data
    ///
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> from bytewax.testing import TestingInput, run_main
    /// >>> from bytewax.connectors.stdio import StdOutput
    /// >>> flow = Dataflow()
    /// >>> inp = [
    /// ...     {"user": "a", "type": "login"},
    /// ...     {"user": "a", "type": "post"},
    /// ...     {"user": "b", "type": "login"},
    /// ...     {"user": "b", "type": "logout"},
    /// ...     {"user": "a", "type": "logout"},
    /// ... ]
    /// >>> flow.input("inp", TestingInput(inp))
    /// >>> def user_as_key(event):
    /// ...     return event["user"], [event]
    /// >>> flow.map(user_as_key)
    /// >>> def extend_session(session, events):
    /// ...     session.extend(events)
    /// ...     return session
    /// >>> def session_complete(session):
    /// ...     return any(event["type"] == "logout" for event in session)
    /// >>> flow.reduce("sessionizer", extend_session, session_complete)
    /// >>> flow.output("out", StdOutput())
    /// >>> run_main(flow)
    /// ('b', [{'user': 'b', 'type': 'login'}, {'user': 'b', 'type': 'logout'}])
    /// ('a', [{'user': 'a', 'type': 'login'}, {'user': 'a', 'type': 'post'},
    ///        {'user': 'a', 'type': 'logout'}])
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   reducer:
    ///       `reducer(accumulator: Any, value: Any) =>
    ///       updated_accumulator: Any`
    ///   is_complete:
    ///       `is_complete(updated_accumulator: Any) =>
    ///       should_emit: bool`
    fn reduce(&mut self, step_id: StepId, reducer: TdPyCallable, is_complete: TdPyCallable) {
        self.steps.push(Step::Reduce {
            step_id,
            reducer,
            is_complete,
        });
    }

    /// Fold window lets you combine all items for a key within a
    /// window into an accumulator, using a function to build its initial value.
    ///
    /// It is like `Dataflow.reduce_window` but uses a function to
    /// build the initial value.
    ///
    /// It is a stateful operator. It requires the input stream
    /// has items that are `(key: str, value)` tuples so we can ensure
    /// that all relevant values are routed to the relevant state. It
    /// also requires a step ID to recover the correct state.
    ///
    /// It calls two functions:
    ///
    /// - A **builder** function which is called the first time a key appears
    ///   and is expected to return the empty state for that key.
    ///
    /// - A **folder** which combines a new value with an accumulator.
    ///   The accumulator is initially the output of the builder function.
    ///   Values will be passed in window order, but no order
    ///   is defined within a window.
    ///
    /// It emits `(key, accumulator)` tuples downstream when the window closes
    ///
    ///
    /// >>> from datetime import datetime, timedelta, timezone
    /// >>> from bytewax.dataflow import Dataflow
    /// >>> from bytewax.testing import run_main, TestingInput, TestingOutput
    /// >>> from bytewax.window import TumblingWindow, EventClockConfig
    /// >>> align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    /// >>>
    /// >>> flow = Dataflow()
    /// >>>
    /// >>> inp = [
    /// ...     ("ALL", {"time": align_to, "val": "a"}),
    /// ...     ("ALL", {"time": align_to + timedelta(seconds=4), "val": "b"}),
    /// ...     ("ALL", {"time": align_to + timedelta(seconds=8), "val": "c"}),
    /// ...     # The 10 second window should close just before processing this item.
    /// ...     ("ALL", {"time": align_to + timedelta(seconds=12), "val": "d"}),
    /// ...     ("ALL", {"time": align_to + timedelta(seconds=16), "val": "e"})
    /// ... ]
    /// >>>
    /// >>> flow.input("inp", TestingInput(inp))
    /// >>>
    /// >>> clock_config = EventClockConfig(
    /// ...     lambda e: e["time"], wait_for_system_duration=timedelta(seconds=0)
    /// ... )
    /// >>> window_config = TumblingWindow(length=timedelta(seconds=10), align_to=align_to)
    /// >>>
    /// >>> def add(acc, x):
    /// ...     acc.append(x["val"])
    /// ...     return acc
    /// >>>
    /// >>> flow.fold_window("sum", clock_config, window_config, list, add)
    /// >>>
    /// >>> out = []
    /// >>> flow.output("out", TestingOutput(out))
    /// >>>
    /// >>> run_main(flow)
    /// >>>
    /// >>> assert sorted(out) == sorted([("ALL", ["a", "b", "c"]), ("ALL", ["d", "e"])])
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   clock_config (bytewax.window.ClockConfig):
    ///       Clock config to use. See `bytewax.window`.
    ///   window_config (bytewax.window.WindowConfig):
    ///       Windower config to use. See `bytewax.window`.
    ///   builder:
    ///       `builder(key: Any) => initial_accumulator: Any`
    ///   folder:
    ///       `folder(accumulator: Any, value: Any) => updated_accumulator: Any`
    fn fold_window(
        &mut self,
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
        builder: TdPyCallable,
        folder: TdPyCallable,
    ) {
        self.steps.push(Step::FoldWindow {
            step_id,
            clock_config,
            window_config,
            builder,
            folder,
        });
    }

    /// Reduce window lets you combine all items for a key within a
    /// window into an accumulator.
    ///
    /// It is like `Dataflow.reduce` but marks the
    /// accumulator as complete automatically at the end of each
    /// window.
    ///
    /// It is a stateful operator. It requires the input stream
    /// has items that are `(key: str, value)` tuples so we can ensure
    /// that all relevant values are routed to the relevant state. It
    /// also requires a step ID to recover the correct state.
    ///
    /// It calls a **reducer** function which combines two values. The
    /// accumulator is initially the first value seen for a key. Values
    /// will be passed in arbitrary order. If there is only a single
    /// value for a key in this window, this function will not be
    /// called.
    ///
    /// It emits `(key, accumulator)` tuples downstream at the end of
    /// each window.
    ///
    /// If the ordering of values is crucial, group in this operator,
    /// then sort afterwards.
    ///
    /// Currently, data is permanently allocated per-key. If you have
    /// an ever-growing key space, note this.
    ///
    /// It is commonly used for:
    ///
    /// - Sessionization
    ///
    /// >>> from datetime import datetime, timedelta, timezone
    /// >>> from bytewax.testing import TestingInput, TestingOutput, run_main
    /// >>> from bytewax.window import EventClockConfig, TumblingWindow
    /// >>> align_to = datetime(2022, 1, 1, tzinfo=timezone.utc)
    /// >>> flow = Dataflow()
    /// >>> inp = [
    /// ...     ("b", {"time": align_to, "val": 1}),
    /// ...     ("a", {"time": align_to + timedelta(seconds=4), "val": 1}),
    /// ...     ("a", {"time": align_to + timedelta(seconds=8), "val": 1}),
    /// ...     ("b", {"time": align_to + timedelta(seconds=12), "val": 1}),
    /// ... ]
    /// >>> flow.input("inp", TestingInput(inp))
    /// >>> def add(acc, x):
    /// ...     acc["val"] += x["val"]
    /// ...     return acc
    /// >>> clock_config = EventClockConfig(
    /// ...     lambda e: e["time"], wait_for_system_duration=timedelta(0)
    /// ... )
    /// >>> window_config = TumblingWindow(
    /// ...     length=timedelta(seconds=10), align_to=align_to
    /// ... )
    /// >>> flow.reduce_window("count", clock_config, window_config, add)
    /// >>> def extract_val(key__event):
    /// ...    key, event = key__event
    /// ...    return (key, event["val"])
    /// >>> flow.map(extract_val)
    /// >>> out = []
    /// >>> flow.output("out", TestingOutput(out))
    /// >>> run_main(flow)
    /// >>> assert sorted(out) == sorted([('b', 1), ('a', 2), ('b', 1)])
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   clock_config (bytewax.window.ClockConfig):
    ///       Clock config to use. See `bytewax.window`.
    ///   window_config (bytewax.window.WindowConfig):
    ///       Windower config to use. See `bytewax.window`.
    ///   reducer: `reducer(accumulator: Any, value: Any) =>
    ///       updated_accumulator: Any`
    fn reduce_window(
        &mut self,
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
        reducer: TdPyCallable,
    ) {
        self.steps.push(Step::ReduceWindow {
            step_id,
            clock_config,
            window_config,
            reducer,
        });
    }

    /// Collect window lets emits all items for a key in a window
    /// downstream in sorted order.
    ///
    /// It is a stateful operator. It requires the upstream items are
    /// `(key: str, value)` tuples so we can ensure that all relevant
    /// values are routed to the relevant state. It also requires a
    /// step ID to recover the correct state.
    ///
    /// It emits `(key, list)` tuples downstream at the end of each
    /// window where `list` is sorted by the time assigned by the
    /// clock.
    ///
    /// Currently, data is permanently allocated per-key. If you have
    /// an ever-growing key space, note this.
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   clock_config (bytewax.window.ClockConfig):
    ///       Clock config to use. See `bytewax.window`.
    ///   window_config (bytewax.window.WindowConfig):
    ///       Windower config to use. See `bytewax.window`.
    fn collect_window(
        &mut self,
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
    ) {
        self.steps.push(Step::CollectWindow {
            step_id,
            clock_config,
            window_config,
        });
    }

    /// Stateful map is a one-to-one transformation of values, but
    /// allows you to reference a persistent state for each key when
    /// doing the transformation.
    ///
    /// It is a stateful operator. It requires the input stream
    /// has items that are `(key: str, value)` tuples so we can ensure
    /// that all relevant values are routed to the relevant state. It
    /// also requires a step ID to recover the correct state.
    ///
    /// It calls two functions:
    ///
    /// - A **builder** which returns a new state and will be called
    /// whenever a new key is encountered with the key as a parameter.
    ///
    /// - A **mapper** which transforms values. Values will be passed
    /// in an arbitrary order. If the updated state is `None`, the
    /// state will be forgotten.
    ///
    /// It emits a `(key, updated_value)` tuple downstream for each
    /// input item.
    ///
    /// If the ordering of values is crucial, group beforhand using a
    /// windowing operator with a timeout like `reduce_window`, then
    /// sort, then use this operator.
    ///
    /// It is commonly used for:
    ///
    /// - Anomaly detection
    /// - State machines
    ///
    /// >>> from bytewax.testing import TestingInput, run_main
    /// >>> from bytewax.connectors.stdio import StdOutput
    /// >>> flow = Dataflow()
    /// >>> inp = [
    /// ...     "a",
    /// ...     "a",
    /// ...     "a",
    /// ...     "a",
    /// ...     "b",
    /// ... ]
    /// >>> flow.input("inp", TestingInput(inp))
    /// >>> def self_as_key(item):
    /// ...     return item, item
    /// >>> flow.map(self_as_key)
    /// >>> def build_count():
    /// ...     return 0
    /// >>> def check(running_count, item):
    /// ...     running_count += 1
    /// ...     if running_count == 1:
    /// ...         return running_count, item
    /// ...     else:
    /// ...         return running_count, None
    /// >>> flow.stateful_map("remove_duplicates", build_count, check)
    /// >>> def remove_none_and_key(key_item):
    /// ...     key, item = key_item
    /// ...     if item is None:
    /// ...         return []
    /// ...     else:
    /// ...         return [item]
    /// >>> flow.flat_map(remove_none_and_key)
    /// >>> flow.output("out", StdOutput())
    /// >>> run_main(flow)
    /// a
    /// b
    ///
    /// Args:
    ///   step_id (str):
    ///       Uniquely identifies this step for recovery.
    ///   builder:
    ///       `builder(key: Any) => new_state: Any`
    ///   mapper:
    ///       `mapper(state: Any, value: Any) => (updated_state:
    ///       Any, updated_value: Any)`
    fn stateful_map(&mut self, step_id: StepId, builder: TdPyCallable, mapper: TdPyCallable) {
        self.steps.push(Step::StatefulMap {
            step_id,
            builder,
            mapper,
        });
    }

    fn __json__<'py>(&'py self, py: Python<'py>) -> PyResult<&'py PyDict> {
        let dict = PyDict::new(py);
        dict.set_item("type", "Dataflow")?;
        let steps = PyList::empty(py);
        for step in &self.steps {
            let step_dict = PyDict::new(py);
            match step {
                Step::Redistribute => {
                    step_dict.set_item("type", "Redistribute")?;
                }
                Step::Input { step_id, input } => {
                    step_dict.set_item("type", "Input")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("input", input)?;
                }
                Step::Map { mapper } => {
                    step_dict.set_item("type", "Map")?;
                    step_dict.set_item("mapper", mapper)?;
                }
                Step::FlatMap { mapper } => {
                    step_dict.set_item("type", "FlatMap")?;
                    step_dict.set_item("mapper", mapper)?;
                }
                Step::Filter { predicate } => {
                    step_dict.set_item("type", "Filter")?;
                    step_dict.set_item("predicate", predicate)?;
                }
                Step::FilterMap { mapper } => {
                    step_dict.set_item("type", "FilterMap")?;
                    step_dict.set_item("mapper", mapper)?;
                }
                Step::FoldWindow {
                    step_id,
                    clock_config,
                    window_config,
                    builder,
                    folder,
                } => {
                    step_dict.set_item("type", "FoldWindow")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("clock_config", clock_config)?;
                    step_dict.set_item("window_config", window_config)?;
                    step_dict.set_item("builder", builder)?;
                    step_dict.set_item("folder", folder)?;
                }
                Step::Inspect { inspector } => {
                    step_dict.set_item("type", "Inspect")?;
                    step_dict.set_item("inspector", inspector)?;
                }
                Step::InspectWorker { inspector } => {
                    step_dict.set_item("type", "Inspect")?;
                    step_dict.set_item("inspector", inspector)?;
                }
                Step::InspectEpoch { inspector } => {
                    step_dict.set_item("type", "InspectEpoch")?;
                    step_dict.set_item("inspector", inspector)?;
                }
                Step::Reduce {
                    step_id,
                    reducer,
                    is_complete,
                } => {
                    step_dict.set_item("type", "Reduce")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("reducer", reducer)?;
                    step_dict.set_item("is_complete", is_complete)?;
                }
                Step::ReduceWindow {
                    step_id,
                    clock_config,
                    window_config,
                    reducer,
                } => {
                    step_dict.set_item("type", "ReduceWindow")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("clock_config", clock_config)?;
                    step_dict.set_item("window_config", window_config)?;
                    step_dict.set_item("reducer", reducer)?;
                }
                Step::CollectWindow {
                    step_id,
                    clock_config,
                    window_config,
                } => {
                    step_dict.set_item("type", "CollectWindow")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("clock_config", clock_config)?;
                    step_dict.set_item("window_config", window_config)?;
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    step_dict.set_item("type", "StatefulMap")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("builder", builder)?;
                    step_dict.set_item("mapper", mapper)?;
                }
                Step::Output { step_id, output } => {
                    step_dict.set_item("type", "Output")?;
                    step_dict.set_item("step_id", step_id)?;
                    step_dict.set_item("output", output)?;
                }
            }
            steps.append(step_dict)?;
        }
        dict.set_item("steps", steps)?;
        Ok(dict)
    }
}

/// The definition of one step in a Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// See
/// <https://docs.rs/timely/latest/timely/dataflow/operators/index.html>
/// for Timely's operators. We try to keep the same semantics here.
#[derive(Clone)]
pub(crate) enum Step {
    Redistribute,
    Input {
        step_id: StepId,
        input: Input,
    },
    Map {
        mapper: TdPyCallable,
    },
    FlatMap {
        mapper: TdPyCallable,
    },
    Filter {
        predicate: TdPyCallable,
    },
    FilterMap {
        mapper: TdPyCallable,
    },
    FoldWindow {
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
        builder: TdPyCallable,
        folder: TdPyCallable,
    },
    Inspect {
        inspector: TdPyCallable,
    },
    InspectWorker {
        inspector: TdPyCallable,
    },
    InspectEpoch {
        inspector: TdPyCallable,
    },
    Reduce {
        step_id: StepId,
        reducer: TdPyCallable,
        is_complete: TdPyCallable,
    },
    ReduceWindow {
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
        reducer: TdPyCallable,
    },
    CollectWindow {
        step_id: StepId,
        clock_config: Py<ClockConfig>,
        window_config: Py<WindowConfig>,
    },
    StatefulMap {
        step_id: StepId,
        builder: TdPyCallable,
        mapper: TdPyCallable,
    },
    Output {
        step_id: StepId,
        output: Output,
        min_batch_size: usize,
        timeout: Option<chrono::Duration>,
    },
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    Ok(())
}
