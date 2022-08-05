//! Internal definition of Bytewax dataflows.
//!
//! For a user-centric version of how to define a dataflow, read the
//! `bytewax.dataflow` Python module docstring. Read that first.

use crate::inputs::InputConfig;
use crate::outputs::OutputConfig;
use crate::pyo3_extensions::TdPyCallable;
use crate::window::ClockConfig;
use crate::window::WindowConfig;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::Deserialize;
use serde::Serialize;
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::sqlite::SqliteArgumentValue;
use sqlx::sqlite::SqliteTypeInfo;
use sqlx::sqlite::SqliteValueRef;
use sqlx::Decode;
use sqlx::Encode;
use sqlx::Sqlite;
use sqlx::Type;
use std::borrow::Cow;
use std::fmt::Display;

/// Newtype representing the unique name of a step in a dataflow.
///
/// Used as a key for looking up relevant state for different steps
/// surrounding recovery.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) struct StepId(String);

impl<'source> FromPyObject<'source> for StepId {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(StepId(ob.extract()?))
    }
}

impl IntoPy<PyObject> for StepId {
    fn into_py(self, py: Python) -> Py<PyAny> {
        PyString::new(py, &self.0).into()
    }
}

impl Display for StepId {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        fmt.write_str(&self.0)
    }
}

impl From<String> for StepId {
    fn from(s: String) -> Self {
        StepId(s)
    }
}

impl From<StepId> for String {
    fn from(step_id: StepId) -> Self {
        step_id.0
    }
}

impl Type<Sqlite> for StepId {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<Sqlite>>::type_info()
    }
}

impl<'q> Encode<'q, Sqlite> for StepId {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0)));
        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.0.clone())));
        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for StepId {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <String as Decode<Sqlite>>::decode(value)?;
        Ok(Self(value))
    }
}

/// A definition of a Bytewax dataflow graph.
///
/// Use the methods defined on this class to add steps with operators
/// of the same name.
///
/// See the execution entry points in `bytewax.execution` to run.
///
/// See `bytewax.inputs` for more information on how input works.
///
/// Args:
///
///     input_config: Initial source of input. See `bytewax.inputs`.
///
// TODO: Right now this is just a linear dataflow only.
#[pyclass(module = "bytewax.dataflow")]
#[pyo3(text_signature = "(input_config)")]
pub(crate) struct Dataflow {
    pub(crate) input_config: Py<InputConfig>,
    pub(crate) steps: Vec<Step>,
}

#[pymethods]
impl Dataflow {
    #[new]
    #[args(input_config)]
    fn new(input_config: Py<InputConfig>) -> Self {
        Self {
            input_config,
            steps: Vec::new(),
        }
    }

    fn __getstate__(&self) -> (&str, Py<InputConfig>, Vec<Step>) {
        ("Dataflow", self.input_config.clone(), self.steps.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (Py<InputConfig>,) {
        (InputConfig::pickle_new(py),)
    }

    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("Dataflow", input_config, steps)) = state.extract() {
            self.input_config = input_config;
            self.steps = steps;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for Dataflow: {state:?}"
            )))
        }
    }

    /// Capture is how you specify output of a dataflow.
    ///
    /// At least one capture is required on every dataflow.
    ///
    /// It emits items downstream unmodified; you can capture midway
    /// through a dataflow.
    ///
    /// See `bytewax.outputs` for more information on how output
    /// works.
    ///
    /// >>> flow = Dataflow()
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> sorted(run(flow, inp))
    /// [(0, 0), (1, 1), (2, 2)]
    ///
    /// Args:
    ///
    ///     output_config: Sink to write output. See
    ///         `bytewax.outputs`.
    #[pyo3(text_signature = "(self, output_config)")]
    fn capture(&mut self, output_config: Py<OutputConfig>) {
        self.steps.push(Step::Capture { output_config });
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
    /// >>> def is_odd(item):
    /// ...     return item % 2 != 0
    /// >>> flow = Dataflow()
    /// >>> flow.filter(is_odd)
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> for epoch, item in run(flow, inp):
    /// ...    print(item)
    /// 1
    /// 3
    ///
    /// Args:
    ///
    ///     predicate: `predicate(item: Any) => should_emit: bool`
    #[pyo3(text_signature = "(self, predicate)")]
    fn filter(&mut self, predicate: TdPyCallable) {
        self.steps.push(Step::Filter { predicate });
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
    /// >>> def split_into_words(sentence):
    /// ...     return sentence.split()
    /// >>> flow = Dataflow()
    /// >>> flow.flat_map(split_into_words)
    /// >>> flow.capture()
    /// >>> inp = enumerate(["hello world"])
    /// >>> for epoch, item in run(flow, inp):
    /// ...     print(epoch, item)
    /// 0 hello
    /// 0 world
    ///
    /// Args:
    ///
    ///     mapper: `mapper(item: Any) => emit: Iterable[Any]`
    #[pyo3(text_signature = "(self, mapper)")]
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
    /// >>> def log(item):
    /// ...     print("Saw", item)
    /// >>> flow = Dataflow()
    /// >>> flow.inspect(log)
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> for epoch, item in run(flow, inp):
    /// ...     pass  # Don't print captured output.
    /// Saw 1
    /// Saw 2
    /// Saw 3
    ///
    /// Args:
    ///
    ///     inspector: `inspector(item: Any) => None`
    #[pyo3(text_signature = "(self, inspector)")]
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
    /// >>> def log(epoch, item):
    /// ...    print(f"Saw {item} @ {epoch}")
    /// >>> flow = Dataflow()
    /// >>> flow.inspect_epoch(log)
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> for epoch, item in run(flow, inp):
    /// ...    pass  # Don't print captured output.
    ///
    /// Args:
    ///
    ///     inspector: `inspector(epoch: int, item: Any) => None`
    #[pyo3(text_signature = "(self, inspector)")]
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
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
    /// >>> def add_one(item):
    /// ...     return item + 10
    /// >>> flow = Dataflow()
    /// >>> flow.map(add_one)
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> for epoch, item in run(flow, inp):
    /// ...     print(item)
    /// 10
    /// 11
    /// 12
    ///
    /// Args:
    ///
    ///     mapper: `mapper(item: Any) => updated_item: Any`
    #[pyo3(text_signature = "(self, mapper)")]
    fn map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::Map { mapper });
    }

    /// Reduce lets you combine items for a key into an accumulator in
    /// epoch order.
    ///
    /// It is a stateful operator. It requires the the input stream
    /// has items that are `(key: str, value)` tuples so we can ensure
    /// that all relevant values are routed to the relevant state. It
    /// also requires a step ID to recover the correct state.
    ///
    /// It calls two functions:
    ///
    /// - A **reducer** which combines a new value with an
    /// accumulator. The accumulator is initially the first value seen
    /// for a key. Values will be passed in epoch order, but no order
    /// is defined within an epoch. If there is only a single value
    /// for a key since the last completion, this function will not be
    /// called.
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
    /// It is commonly used for:
    ///
    /// - Sessionization
    /// - Emitting a summary of data that spans epochs
    ///
    /// >>> def user_as_key(event):
    /// ...     return event["user"], [event]
    /// >>> def extend_session(session, events):
    /// ...     session.extend(events)
    /// ...     return session
    /// >>> def session_complete(session):
    /// ...     return any(event["type"] == "logout" for event in session)
    /// >>> flow = Dataflow()
    /// >>> flow.map(user_as_key)
    /// >>> flow.inspect_epoch(lambda epoch, item: print("Saw", item, "@", epoch))
    /// >>> flow.reduce("sessionizer", extend_session, session_complete)
    /// >>> flow.capture()
    /// >>> inp = [
    /// ...     (0, {"user": "a", "type": "login"}),
    /// ...     (1, {"user": "a", "type": "post"}),
    /// ...     (1, {"user": "b", "type": "login"}),
    /// ...     (2, {"user": "a", "type": "logout"}),
    /// ...     (3, {"user": "b", "type": "logout"}),
    /// ... ]
    /// >>> for epoch, item in run(flow, inp):
    /// ...     print(epoch, item)
    ///
    /// Args:
    ///
    ///     step_id: Uniquely identifies this step for recovery.
    ///
    ///     reducer: `reducer(accumulator: Any, value: Any) =>
    ///         updated_accumulator: Any`
    ///
    ///     is_complete: `is_complete(updated_accumulator: Any) =>
    ///         should_emit: bool`
    #[pyo3(text_signature = "(self, step_id, reducer, is_complete)")]
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
    /// It is like `bytewax.Dataflow.reduce_window()` but uses a function to
    /// build the initial value.
    ///
    /// See reduce_window's documentation and the `test_fold_window` test in
    /// `bytewax/pytests/test_window.py` file for more details.
    ///
    /// Args:
    ///
    ///     step_id: Uniquely identifies this step for recovery.
    ///
    ///     clock_config: Clock config to use. See `bytewax.window`.
    ///
    ///     window_config: Windower config to use. See `bytewax.window`.
    ///
    ///     builder: `builder(key: Any) => initial_accumulator: Any`
    ///
    ///     folder: `folder(accumulator: Any, value: Any) => updated_accumulator: Any`
    #[pyo3(text_signature = "(self, step_id, clock_config, window_config, builder, folder)")]
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

    // TODO: Update this example once we have a better idea about
    // input.
    /// Reduce window lets you combine all items for a key within a
    /// window into an accumulator.
    ///
    /// It is like `bytewax.Dataflow.reduce()` but marks the
    /// accumulator as complete automatically at the end of each
    /// window.
    ///
    /// It is a stateful operator. It requires the the input stream
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
    /// It is commonly used for:
    ///
    /// - Counting within windows
    /// - Accumulation within windows
    ///
    /// >>> def add_initial_count(event):
    /// ...     return event["user"], 1
    /// >>> def count(count, event_count):
    /// ...     return count + event_count
    /// >>> flow = Dataflow()
    /// >>> flow.map(add_initial_count)
    /// >>> flow.inspect_epoch(lambda epoch, item: print("Saw", item, "@", epoch))
    /// >>> flow.reduce_epoch(count)
    /// >>> flow.capture()
    /// >>> inp = [
    /// ...     (0, {"user": "a", "type": "login"}),
    /// ...     (0, {"user": "a", "type": "post"}),
    /// ...     (0, {"user": "b", "type": "login"}),
    /// ...     (1, {"user": "b", "type": "post"}),
    /// ... ]
    /// >>> for epoch, item in run(flow, inp):
    /// ...     print(epoch, item)
    /// Saw ('a', 1) @ 0
    /// Saw ('b', 1) @ 0
    /// Saw ('a', 1) @ 0
    /// Saw ('b', 1) @ 1
    /// 0 ('b', 1)
    /// 0 ('a', 2)
    /// 1 ('b', 1)
    ///
    /// Args:
    ///
    ///     step_id: Uniquely identifies this step for recovery.
    ///
    ///     clock_config: Clock config to use. See `bytewax.window`.
    ///
    ///     window_config: Windower config to use. See
    ///         `bytewax.window`.
    ///
    ///     reducer: `reducer(accumulator: Any, value: Any) =>
    ///         updated_accumulator: Any`
    #[pyo3(text_signature = "(self, step_id, clock_config, window_config, reducer)")]
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

    /// Stateful map is a one-to-one transformation of values, but
    /// allows you to reference a persistent state for each key when
    /// doing the transformation.
    ///
    /// It is a stateful operator. It requires the the input stream
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
    /// in epoch order, but no order is defined within an epoch. If
    /// the updated state is `None`, the state will be forgotten.
    ///
    /// It emits a `(key, updated_value)` tuple downstream for each
    /// input item.
    ///
    /// It is commonly used for:
    ///
    /// - Anomaly detection
    /// - State machines
    ///
    /// >>> def self_as_key(item):
    /// ...     return item, item
    /// >>> def build_count(key):
    /// ...     return 0
    /// >>> def check(running_count, item):
    /// ...     running_count += 1
    /// ...     if running_count == 1:
    /// ...         return running_count, item
    /// ...     else:
    /// ...         return running_count, None
    /// >>> def remove_none_and_key(key_item):
    /// ...     key, item = key_item
    /// ...     if item is None:
    /// ...         return []
    /// ...     else:
    /// ...         return [item]
    /// >>> flow = Dataflow()
    /// >>> flow.map(self_as_key)
    /// >>> flow.stateful_map("remove_duplicates", build_count, check)
    /// >>> flow.flat_map(remove_none_and_key)
    /// >>> flow.capture()
    /// >>> inp = [
    /// ...     (0, "a"),
    /// ...     (0, "a"),
    /// ...     (0, "a"),
    /// ...     (1, "a"),
    /// ...     (1, "b"),
    /// ... ]
    /// >>> for epoch, item in run(flow, inp):
    /// ...     print(epoch, item)
    /// 0 a
    /// 1 b
    ///
    /// Args:
    ///
    ///     step_id: Uniquely identifies this step for recovery.
    ///
    ///     builder: `builder(key: Any) => new_state: Any`
    ///
    ///     mapper: `mapper(state: Any, value: Any) => (updated_state:
    ///         Any, updated_value: Any)`
    #[pyo3(text_signature = "(self, step_id, builder, mapper)")]
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
    Map {
        mapper: TdPyCallable,
    },
    FlatMap {
        mapper: TdPyCallable,
    },
    Filter {
        predicate: TdPyCallable,
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
    StatefulMap {
        step_id: StepId,
        builder: TdPyCallable,
        mapper: TdPyCallable,
    },
    Capture {
        output_config: Py<OutputConfig>,
    },
}

/// Decode Steps from Python (name, funcs...) tuples.
///
/// Required for pickling.
impl<'source> FromPyObject<'source> for Step {
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        let tuple: &PySequence = obj.downcast()?;

        if let Ok(("Map", mapper)) = tuple.extract() {
            return Ok(Self::Map { mapper });
        }
        if let Ok(("FlatMap", mapper)) = tuple.extract() {
            return Ok(Self::FlatMap { mapper });
        }
        if let Ok(("Filter", predicate)) = tuple.extract() {
            return Ok(Self::Filter { predicate });
        }
        if let Ok(("FoldWindow", step_id, clock_config, window_config, builder, folder)) =
            tuple.extract()
        {
            return Ok(Self::FoldWindow {
                step_id,
                clock_config,
                window_config,
                builder,
                folder,
            });
        }
        if let Ok(("Inspect", inspector)) = tuple.extract() {
            return Ok(Self::Inspect { inspector });
        }
        if let Ok(("InspectEpoch", inspector)) = tuple.extract() {
            return Ok(Self::InspectEpoch { inspector });
        }
        if let Ok(("Reduce", step_id, reducer, is_complete)) = tuple.extract() {
            return Ok(Self::Reduce {
                step_id,
                reducer,
                is_complete,
            });
        }
        if let Ok(("ReduceWindow", step_id, clock_config, window_config, reducer)) = tuple.extract()
        {
            return Ok(Self::ReduceWindow {
                step_id,
                clock_config,
                window_config,
                reducer,
            });
        }
        if let Ok(("StatefulMap", step_id, builder, mapper)) = tuple.extract() {
            return Ok(Self::StatefulMap {
                step_id,
                builder,
                mapper,
            });
        }
        if let Ok(("Capture", output_config)) = tuple.extract() {
            return Ok(Self::Capture { output_config });
        }

        Err(PyValueError::new_err(format!(
            "bad python repr when unpickling Step: {obj:?}"
        )))
    }
}

/// Represent Steps in Python as (name, funcs...) tuples.
///
/// Required for pickling.
impl IntoPy<PyObject> for Step {
    fn into_py(self, py: Python) -> Py<PyAny> {
        match self {
            Self::Map { mapper } => ("Map", mapper).into_py(py),
            Self::FlatMap { mapper } => ("FlatMap", mapper).into_py(py),
            Self::Filter { predicate } => ("Filter", predicate).into_py(py),
            Self::FoldWindow {
                step_id,
                clock_config,
                window_config,
                builder,
                folder,
            } => (
                "FoldWindow",
                step_id.clone(),
                clock_config,
                window_config,
                builder,
                folder,
            )
                .into_py(py),
            Self::Inspect { inspector } => ("Inspect", inspector).into_py(py),
            Self::InspectEpoch { inspector } => ("InspectEpoch", inspector).into_py(py),
            Self::Reduce {
                step_id,
                reducer,
                is_complete,
            } => ("Reduce", step_id.clone(), reducer, is_complete).into_py(py),
            Self::ReduceWindow {
                step_id,
                clock_config,
                window_config,
                reducer,
            } => (
                "ReduceWindow",
                step_id.clone(),
                clock_config,
                window_config,
                reducer,
            )
                .into_py(py),
            Self::StatefulMap {
                step_id,
                builder,
                mapper,
            } => ("StatefulMap", step_id.clone(), builder, mapper).into_py(py),
            Self::Capture { output_config } => ("Capture", output_config).into_py(py),
        }
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    Ok(())
}
