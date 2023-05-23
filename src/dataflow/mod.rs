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

use std::collections::HashMap;

use crate::common::pickle_extract;
use crate::errors::PythonException;
use crate::inputs::Input;
use crate::outputs::Output;
use crate::pyo3_extensions::TdPyCallable;
use crate::recovery::model::StepId;
use crate::window::clock::ClockConfig;
use crate::window::WindowConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;

/// A definition of a Bytewax dataflow graph.
///
/// Use the methods defined on this class to add steps with operators
/// of the same name.
// TODO: Right now this is just a linear dataflow only.
#[pyclass(module = "bytewax.dataflow")]
#[pyo3(text_signature = "()")]
#[derive(Clone)]
pub(crate) struct Dataflow {
    #[pyo3(get)]
    pub(crate) steps: Vec<Step>,
}

#[pymethods]
impl Dataflow {
    #[new]
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "Dataflow".into_py(py)),
                ("steps", self.steps.clone().into_py(py)),
            ])
        })
    }

    /// Unpickle from a PyDict of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.steps = pickle_extract(dict, "steps").reraise("error unpickling Dataflow steps")?;
        Ok(())
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   input (bytewax.inputs.Input): Input definition.
    #[pyo3(signature = (step_id, input))]
    fn input(&mut self, step_id: StepId, input: Input) {
        self.steps.push(Step::Input { step_id, input });
    }

    /// Spread the work across workers, in a randomic way.
    ///
    /// This operators takes each item and sends it to
    /// one of the available workers.
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
    /// Another case where we see a regression in performance is
    /// if the heavy CPU workload already spawns enough threads
    /// to use all the available cores. In this case multiple
    /// processes trying to compete for the cpu will end up being
    /// slower than doing the work serially.
    /// If the workers run on different machines though, it might
    /// again be a valuable use of the operator.
    ///
    /// So, use this operator with caution, and measure whether you
    /// get an improvement out of it.
    ///
    /// Once the work has been spread to another worker, it will
    /// stay on that worker unless other operators explicitely
    /// move the item again (usually this happens on the output).
    fn spread_to_random_workers(&mut self) {
        self.steps.push(Step::SpreadToRandomWorkers);
    }

    fn send_to_worker(&mut self, worker_id: u64) {
        self.steps.push(Step::SendToWorker { worker_id });
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   output (bytewax.outputs.Output): Output definition.
    #[pyo3(signature = (step_id, output))]
    fn output(&mut self, step_id: StepId, output: Output) {
        self.steps.push(Step::Output { step_id, output });
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
    ///
    ///   predicate: `predicate(item: Any) => should_emit: bool`
    #[pyo3(signature = (predicate))]
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
    #[pyo3(signature = (mapper))]
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
    ///
    ///   mapper: `mapper(item: Any) => emit: Iterable[Any]`
    #[pyo3(signature = (mapper))]
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
    ///
    ///   inspector: `inspector(item: Any) => None`
    #[pyo3(signature = (inspector))]
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
    /// Saw 0 @ 0
    /// Saw 1 @ 1
    /// Saw 2 @ 2
    ///
    /// Args:
    ///
    ///   inspector: `inspector(epoch: int, item: Any) => None`
    #[pyo3(signature = (inspector))]
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
    }

    #[pyo3(text_signature = "(self, inspector)")]
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
    ///
    ///   mapper: `mapper(item: Any) => updated_item: Any`
    #[pyo3(signature = (mapper))]
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   reducer: `reducer(accumulator: Any, value: Any) =>
    ///       updated_accumulator: Any`
    ///
    ///   is_complete: `is_complete(updated_accumulator: Any) =>
    ///       should_emit: bool`
    #[pyo3(signature = (step_id, reducer, is_complete))]
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
    ///
    ///   step_id: Uniquely identifies this step for recovery.
    ///
    ///   clock_config: Clock config to use. See `bytewax.window`.
    ///
    ///   window_config: Windower config to use. See `bytewax.window`.
    ///
    ///   builder: `builder(key: Any) => initial_accumulator: Any`
    ///
    ///   folder: `folder(accumulator: Any, value: Any) => updated_accumulator: Any`
    #[pyo3(signature = (step_id, clock_config, window_config, builder, folder))]
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   clock_config (bytewax.window.ClockConfig): Clock config to
    ///       use. See `bytewax.window`.
    ///
    ///   window_config (bytewax.window.WindowConfig): Windower
    ///       config to use. See `bytewax.window`.
    ///
    ///   reducer: `reducer(accumulator: Any, value: Any) =>
    ///       updated_accumulator: Any`
    #[pyo3(signature = (step_id, clock_config, window_config, reducer))]
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   clock_config (bytewax.window.ClockConfig): Clock config to
    ///       use. See `bytewax.window`.
    ///
    ///   window_config (bytewax.window.WindowConfig): Windower
    ///       config to use. See `bytewax.window`.
    #[pyo3(signature = (step_id, clock_config, window_config))]
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
    ///
    ///   step_id (str): Uniquely identifies this step for recovery.
    ///
    ///   builder: `builder(key: Any) => new_state: Any`
    ///
    ///   mapper: `mapper(state: Any, value: Any) => (updated_state:
    ///       Any, updated_value: Any)`
    #[pyo3(signature = (step_id, builder, mapper))]
    fn stateful_map(&mut self, step_id: StepId, builder: TdPyCallable, mapper: TdPyCallable) {
        self.steps.push(Step::StatefulMap {
            step_id,
            builder,
            mapper,
        });
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
    SpreadToRandomWorkers,
    SendToWorker {
        worker_id: u64,
    },
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
    },
}

/// Decode Steps from Python (name, funcs...) dict.
///
/// Required for pickling.
impl<'source> FromPyObject<'source> for Step {
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        let dict: &PyDict = obj.downcast()?;
        let step: &str = dict
            .get_item("type")
            .expect("Unable to extract step, missing `type` field.")
            .extract()?;
        match step {
            "Input" => Ok(Self::Input {
                step_id: pickle_extract(dict, "step_id")?,
                input: pickle_extract(dict, "input")?,
            }),
            "Map" => Ok(Self::Map {
                mapper: pickle_extract(dict, "mapper")?,
            }),
            "FlatMap" => Ok(Self::FlatMap {
                mapper: pickle_extract(dict, "mapper")?,
            }),
            "Filter" => Ok(Self::Filter {
                predicate: pickle_extract(dict, "predicate")?,
            }),
            "FilterMap" => Ok(Self::FilterMap {
                mapper: pickle_extract(dict, "mapper")?,
            }),
            "FoldWindow" => Ok(Self::FoldWindow {
                step_id: pickle_extract(dict, "step_id")?,
                clock_config: pickle_extract(dict, "clock_config")?,
                window_config: pickle_extract(dict, "window_config")?,
                builder: pickle_extract(dict, "builder")?,
                folder: pickle_extract(dict, "folder")?,
            }),
            "Inspect" => Ok(Self::Inspect {
                inspector: pickle_extract(dict, "inspector")?,
            }),
            "InspectWorker" => Ok(Self::InspectWorker {
                inspector: pickle_extract(dict, "inspector")?,
            }),
            "InspectEpoch" => Ok(Self::InspectEpoch {
                inspector: pickle_extract(dict, "inspector")?,
            }),
            "Reduce" => Ok(Self::Reduce {
                step_id: pickle_extract(dict, "step_id")?,
                reducer: pickle_extract(dict, "reducer")?,
                is_complete: pickle_extract(dict, "is_complete")?,
            }),
            "ReduceWindow" => Ok(Self::ReduceWindow {
                step_id: pickle_extract(dict, "step_id")?,
                clock_config: pickle_extract(dict, "clock_config")?,
                window_config: pickle_extract(dict, "window_config")?,
                reducer: pickle_extract(dict, "reducer")?,
            }),
            "CollectWindow" => Ok(Self::CollectWindow {
                step_id: pickle_extract(dict, "step_id")?,
                clock_config: pickle_extract(dict, "clock_config")?,
                window_config: pickle_extract(dict, "window_config")?,
            }),
            "StatefulMap" => Ok(Self::StatefulMap {
                step_id: pickle_extract(dict, "step_id")?,
                builder: pickle_extract(dict, "builder")?,
                mapper: pickle_extract(dict, "mapper")?,
            }),
            "Output" => Ok(Self::Output {
                step_id: pickle_extract(dict, "step_id")?,
                output: pickle_extract(dict, "output")?,
            }),
            &_ => Err(PyValueError::new_err(format!(
                "bad python repr when unpickling Step: {dict:?}"
            ))),
        }
        .reraise("error unpickling Dataflow step")
    }
}

/// Represent Steps in Python as (name, funcs...) tuples.
///
/// Required for pickling.
impl IntoPy<PyObject> for Step {
    fn into_py(self, py: Python) -> Py<PyAny> {
        match self {
            Self::SpreadToRandomWorkers => {
                HashMap::from([("type", IntoPy::<PyObject>::into_py("Broadcast", py))]).into_py(py)
            }
            Self::SendToWorker { worker_id } => HashMap::from([
                ("type", "Broadcast".into_py(py)),
                ("worker_id", worker_id.into_py(py)),
            ])
            .into_py(py),
            Self::Input { step_id, input } => HashMap::from([
                ("type", "Input".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("input", input.into_py(py)),
            ])
            .into_py(py),
            Self::Map { mapper } => {
                HashMap::from([("type", "Map".into_py(py)), ("mapper", mapper.into_py(py))])
                    .into_py(py)
            }
            Self::FlatMap { mapper } => HashMap::from([
                ("type", "FlatMap".into_py(py)),
                ("mapper", mapper.into_py(py)),
            ])
            .into_py(py),
            Self::Filter { predicate } => HashMap::from([
                ("type", "Filter".into_py(py)),
                ("predicate", predicate.into_py(py)),
            ])
            .into_py(py),
            Self::FilterMap { mapper } => HashMap::from([
                ("type", "FilterMap".into_py(py)),
                ("mapper", mapper.into_py(py)),
            ])
            .into_py(py),
            Self::FoldWindow {
                step_id,
                clock_config,
                window_config,
                builder,
                folder,
            } => HashMap::from([
                ("type", "FoldWindow".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("clock_config", clock_config.into_py(py)),
                ("window_config", window_config.into_py(py)),
                ("builder", builder.into_py(py)),
                ("folder", folder.into_py(py)),
            ])
            .into_py(py),
            Self::Inspect { inspector } => HashMap::from([
                ("type", "Inspect".into_py(py)),
                ("inspector", inspector.into_py(py)),
            ])
            .into_py(py),
            Self::InspectWorker { inspector } => HashMap::from([
                ("type", "Inspect".into_py(py)),
                ("inspector", inspector.into_py(py)),
            ])
            .into_py(py),
            Self::InspectEpoch { inspector } => HashMap::from([
                ("type", "InspectEpoch".into_py(py)),
                ("inspector", inspector.into_py(py)),
            ])
            .into_py(py),
            Self::Reduce {
                step_id,
                reducer,
                is_complete,
            } => HashMap::from([
                ("type", "Reduce".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("reducer", reducer.into_py(py)),
                ("is_complete", is_complete.into_py(py)),
            ])
            .into_py(py),
            Self::ReduceWindow {
                step_id,
                clock_config,
                window_config,
                reducer,
            } => HashMap::from([
                ("type", "ReduceWindow".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("clock_config", clock_config.into_py(py)),
                ("window_config", window_config.into_py(py)),
                ("reducer", reducer.into_py(py)),
            ])
            .into_py(py),
            Self::CollectWindow {
                step_id,
                clock_config,
                window_config,
            } => HashMap::from([
                ("type", "CollectWindow".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("clock_config", clock_config.into_py(py)),
                ("window_config", window_config.into_py(py)),
            ])
            .into_py(py),
            Self::StatefulMap {
                step_id,
                builder,
                mapper,
            } => HashMap::from([
                ("type", "StatefulMap".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("builder", builder.into_py(py)),
                ("mapper", mapper.into_py(py)),
            ])
            .into_py(py),
            Self::Output { step_id, output } => HashMap::from([
                ("type", "Output".into_py(py)),
                ("step_id", step_id.into_py(py)),
                ("output", output.into_py(py)),
            ])
            .into_py(py),
        }
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    Ok(())
}
