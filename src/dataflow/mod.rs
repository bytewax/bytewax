use crate::operators::build;
use crate::operators::check_complete;
use crate::operators::stateful_map;
use crate::operators::Reduce;
use crate::recovery::NoOpRecoveryStore;
use crate::recovery::RecoveryConfig;
use crate::recovery::RecoveryStore;
use crate::recovery::SqliteRecoveryConfig;
use crate::recovery::SqliteRecoveryStore;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;

use std::collections::HashMap;
use timely::dataflow::*;

use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::*;

use crate::execution::Pump;
use crate::operators::{
    capture, filter, flat_map, inspect, inspect_epoch, map, reduce, reduce_epoch,
    reduce_epoch_local, StatefulMap,
};
use crate::pyo3_extensions::{hash, lift_2tuple, wrap_2tuple, TdPyAny, TdPyCallable, TdPyIterator};

/// A definition of a Bytewax dataflow graph.
///
/// Use the methods defined on this class to add steps with operators
/// of the same name.
///
/// See the execution functions in `bytewax` to run.
///
/// TODO: Right now this is just a linear dataflow only.
#[pyclass(module = "bytewax")] // Required to support pickling.
#[pyo3(text_signature = "()")]
pub(crate) struct Dataflow {
    steps: Vec<Step>,
}

#[pymethods]
impl Dataflow {
    #[new]
    fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Pickle a Dataflow as a list of the steps.
    fn __getstate__(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.steps.to_object(py))
    }

    /// Unpickle a Dataflow as a list of the steps.
    fn __setstate__(&mut self, py: Python, state: Py<PyAny>) -> PyResult<()> {
        self.steps = state.as_ref(py).extract()?;
        Ok(())
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
    ///     mapper - `mapper(item: Any) => updated_item: Any`
    #[pyo3(text_signature = "(self, mapper)")]
    fn map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::Map { mapper });
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
    ///     mapper - `mapper(item: Any) => emit: Iterable[Any]`
    #[pyo3(text_signature = "(self, mapper)")]
    fn flat_map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::FlatMap { mapper });
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
    ///     predicate - `predicate(item: Any) => should_emit: bool`
    #[pyo3(text_signature = "(self, predicate)")]
    fn filter(&mut self, predicate: TdPyCallable) {
        self.steps.push(Step::Filter { predicate });
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
    ///     inspector - `inspector(item: Any) => None`
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
    ///     inspector - `inspector(epoch: int, item: Any) => None`
    #[pyo3(text_signature = "(self, inspector)")]
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
    }

    /// Reduce lets you combine items for a key into an aggregator in
    /// epoch order.
    ///
    /// It is a stateful operator. It requires the the input stream
    /// has items that are `(key, value)` tuples so we can ensure that
    /// all relevant values are routed to the relevant aggregator.
    ///
    /// It is a recoverable operator. It requires a step ID to recover
    /// the correct state.
    ///
    /// It calls two functions:
    ///
    /// - A **reducer** which combines a new value with an
    /// aggregator. The aggregator is initially the first value seen
    /// for a key. Values will be passed in epoch order, but no order
    /// is defined within an epoch. If there is only a single value
    /// for a key since the last completion, this function will not be
    /// called.
    ///
    /// - An **is complete** function which returns `True` if the most
    /// recent `(key, aggregator)` should be emitted downstream and
    /// the aggregator for that key forgotten. If there was only a
    /// single value for a key, it is passed in as the aggregator
    /// here.
    ///
    /// It emits `(key, aggregator)` tuples downstream when the is
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
    /// >>> flow.reduce(extend_session, session_complete)
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
    ///     step_id - Uniquely identifies this step for recovery.
    ///
    ///     reducer - `reducer(aggregator: Any, value: Any) =>
    ///         updated_aggregator: Any`
    ///
    ///     is_complete - `is_complete(updated_aggregator: Any) =>
    ///         should_emit: bool`
    #[pyo3(text_signature = "(self, step_id, reducer, is_complete)")]
    fn reduce(&mut self, step_id: String, reducer: TdPyCallable, is_complete: TdPyCallable) {
        self.steps.push(Step::Reduce {
            step_id,
            reducer,
            is_complete,
        });
    }

    /// Reduce epoch lets you combine all items for a key within an
    /// epoch into an aggregator.
    ///
    /// It is like `bytewax.Dataflow.reduce()` but marks the
    /// aggregator as complete automatically at the end of each epoch.
    ///
    /// It is a stateful operator. it requires the the input stream
    /// has items that are `(key, value)` tuples so we can ensure that
    /// all relevant values are routed to the relevant aggregator.
    ///
    /// It calls a **reducer** function which combines two values. The
    /// aggregator is initially the first value seen for a key. Values
    /// will be passed in arbitrary order. If there is only a single
    /// value for a key in this epoch, this function will not be
    /// called.
    ///
    /// It emits `(key, aggregator)` tuples downstream at the end of
    /// each epoch.
    ///
    /// It is commonly used for:
    ///
    /// - Counting within epochs
    /// - Aggregation within epochs
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
    ///     reducer - `reducer(aggregator: Any, value: Any) =>
    ///         updated_aggregator: Any`
    #[pyo3(text_signature = "(self, reducer)")]
    fn reduce_epoch(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpoch { reducer });
    }

    /// Reduce epoch local lets you combine all items for a key within
    /// an epoch _on a single worker._
    ///
    /// It is exactly like `bytewax.Dataflow.reduce_epoch()` but does
    /// _not_ ensure all values for a key are routed to the same
    /// worker and thus there is only one output aggregator per key.
    ///
    /// You should use `bytewax.Dataflow.reduce_epoch()` unless you
    /// need a network-overhead optimization and some later step does
    /// full aggregation.
    ///
    /// It is only used for performance optimziation.
    ///
    /// Args:
    ///
    ///     reducer - `reducer(aggregator: Any, value: Any) =>
    ///         updated_aggregator: Any`
    #[pyo3(text_signature = "(self, reducer)")]
    fn reduce_epoch_local(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpochLocal { reducer });
    }

    /// Stateful map is a one-to-one transformation of values in
    /// `(key, value)` pairs, but allows you to reference a persistent
    /// state for each key when doing the transformation.
    ///
    /// It is a stateful operator. It requires the the input stream
    /// has items that are `(key, value)` tuples so we can ensure that
    /// all relevant values are routed to the relevant state.
    ///
    /// It is a recoverable operator. It requires a step ID to recover
    /// the correct state.
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
    /// >>> flow.stateful_map(build_count, check)
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
    ///     step_id -  Uniquely identifies this step for recovery.
    ///
    ///     builder - `builder(key: Any) => new_state: Any`
    ///
    ///     mapper - `mapper(state: Any, value: Any) =>
    ///         (updated_state: Any, updated_value: Any)`
    #[pyo3(text_signature = "(self, step_id, builder, mapper)")]
    fn stateful_map(&mut self, step_id: String, builder: TdPyCallable, mapper: TdPyCallable) {
        self.steps.push(Step::StatefulMap {
            step_id,
            builder,
            mapper,
        });
    }

    /// Capture is how you specify output of a dataflow.
    ///
    /// At least one capture is required on every dataflow.
    ///
    /// It emits items downstream unmodified; you can capture midway
    /// through a dataflow.
    ///
    /// Whenever an item flows into a capture operator, the [output
    /// handler](./execution#builders) of the worker is called with
    /// that item and epoch. For `bytewax.run()` and
    /// `bytewax.run_cluster()` output handlers are setup for you that
    /// return the output as the return value.
    ///
    /// There are no guarantees on the order that output is passed to
    /// the output handler. Read the attached epoch to discern order.
    ///
    /// >>> flow = Dataflow()
    /// >>> flow.capture()
    /// >>> inp = enumerate(range(3))
    /// >>> sorted(run(flow, inp))
    /// [(0, 0), (1, 1), (2, 2)]
    #[pyo3(text_signature = "(self)")]
    fn capture(&mut self) {
        self.steps.push(Step::Capture {});
    }
}

/// The definition of one step in a Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// See
/// <https://docs.rs/timely/latest/timely/dataflow/operators/index.html>
/// for Timely's operators. We try to keep the same semantics here.
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
    Inspect {
        inspector: TdPyCallable,
    },
    InspectEpoch {
        inspector: TdPyCallable,
    },
    Reduce {
        step_id: String,
        reducer: TdPyCallable,
        is_complete: TdPyCallable,
    },
    ReduceEpoch {
        reducer: TdPyCallable,
    },
    ReduceEpochLocal {
        reducer: TdPyCallable,
    },
    StatefulMap {
        step_id: String,
        builder: TdPyCallable,
        mapper: TdPyCallable,
    },
    Capture {},
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
        if let Ok(("ReduceEpoch", reducer)) = tuple.extract() {
            return Ok(Self::ReduceEpoch { reducer });
        }
        if let Ok(("ReduceEpochLocal", reducer)) = tuple.extract() {
            return Ok(Self::ReduceEpochLocal { reducer });
        }
        if let Ok(("StatefulMap", step_id, builder, mapper)) = tuple.extract() {
            return Ok(Self::StatefulMap {
                step_id,
                builder,
                mapper,
            });
        }
        if let Ok(("Capture",)) = tuple.extract() {
            return Ok(Self::Capture {});
        }

        Err(PyValueError::new_err(format!(
            "bad python repr when unpickling Step: {obj:?}"
        )))
    }
}

/// Represent Steps in Python as (name, funcs...) tuples.
///
/// Required for pickling.
impl ToPyObject for Step {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        match self {
            Self::Map { mapper } => ("Map", mapper).to_object(py),
            Self::FlatMap { mapper } => ("FlatMap", mapper).to_object(py),
            Self::Filter { predicate } => ("Filter", predicate).to_object(py),
            Self::Inspect { inspector } => ("Inspect", inspector).to_object(py),
            Self::InspectEpoch { inspector } => ("InspectEpoch", inspector).to_object(py),
            Self::Reduce {
                step_id,
                reducer,
                is_complete,
            } => ("Reduce", step_id, reducer, is_complete).to_object(py),
            Self::ReduceEpoch { reducer } => ("ReduceEpoch", reducer).to_object(py),
            Self::ReduceEpochLocal { reducer } => ("ReduceEpochLocal", reducer).to_object(py),
            Self::StatefulMap {
                step_id,
                builder,
                mapper,
            } => ("StatefulMap", step_id, builder, mapper).to_object(py),
            Self::Capture {} => ("Capture",).to_object(py),
        }
        .into()
    }
}

pub(crate) fn build_dataflow<A>(
    timely_worker: &mut timely::worker::Worker<A>,
    py: Python,
    flow: &Dataflow,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
    recovery_config: Option<Py<RecoveryConfig>>,
) -> Result<(Pump, ProbeHandle<u64>), String>
where
    A: timely::communication::Allocate,
{
    let worker_index = timely_worker.index();
    let worker_count = timely_worker.peers();

    timely_worker.dataflow(|scope| {
        let mut timely_input = InputHandle::new();
        let mut end_of_steps_probe = ProbeHandle::new();
        let mut has_capture = false;
        let mut stream = timely_input.to_stream(scope);

        let worker_input: TdPyIterator = input_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        let worker_output: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();

        let recovery_store: Box<dyn RecoveryStore> = match recovery_config {
            None => Box::new(NoOpRecoveryStore::new()),
            Some(recovery_config) => {
                let recovery_config = recovery_config.as_ref(py);
                if let Ok(sqlite_recovery_config) =
                    recovery_config.downcast::<PyCell<SqliteRecoveryConfig>>()
                {
                    let sqlite_recovery_config = sqlite_recovery_config.borrow();
                    Box::new(SqliteRecoveryStore::new(sqlite_recovery_config))
                } else {
                    let pytype = recovery_config.get_type();
                    return Err(format!("Unknown recovery_config type: {pytype}"));
                }
            }
        };

        let steps = &flow.steps;
        for step in steps {
            match step {
                Step::Map { mapper } => {
                    // All these closure lifetimes are static, so tell
                    // Python's GC that there's another pointer to the
                    // mapping function that's going to hang around
                    // for a while when it's moved into the closure.
                    let mapper = mapper.clone_ref(py);
                    stream = stream.map(move |item| map(&mapper, item));
                }
                Step::FlatMap { mapper } => {
                    let mapper = mapper.clone_ref(py);
                    stream = stream.flat_map(move |item| flat_map(&mapper, item));
                }
                Step::Filter { predicate } => {
                    let predicate = predicate.clone_ref(py);
                    stream = stream.filter(move |item| filter(&predicate, item));
                }
                Step::Inspect { inspector } => {
                    let inspector = inspector.clone_ref(py);
                    stream = stream.inspect(move |item| inspect(&inspector, item));
                }
                Step::InspectEpoch { inspector } => {
                    let inspector = inspector.clone_ref(py);
                    stream = stream
                        .inspect_time(move |epoch, item| inspect_epoch(&inspector, epoch, item));
                }
                Step::Reduce {
                    step_id,
                    reducer,
                    is_complete,
                } => {
                    let reducer = reducer.clone_ref(py);
                    let is_complete = is_complete.clone_ref(py);
                    stream = stream
                        .map(lift_2tuple)
                        .reduce(
                            recovery_store.for_step(step_id),
                            move |key, aggregator, value| reduce(&reducer, key, aggregator, value),
                            move |key, aggregator| check_complete(&is_complete, key, aggregator),
                            hash,
                        )
                        .map(wrap_2tuple);
                }
                Step::ReduceEpoch { reducer } => {
                    let reducer = reducer.clone_ref(py);
                    stream = stream.map(lift_2tuple).aggregate(
                        move |key, value, aggregator: &mut Option<TdPyAny>| {
                            reduce_epoch(&reducer, aggregator, key, value);
                        },
                        move |key, aggregator: Option<TdPyAny>| {
                            // Aggregator will only exist for keys
                            // that exist, so it will have been filled
                            // into Some(value) above.
                            wrap_2tuple((key, aggregator.unwrap()))
                        },
                        hash,
                    );
                }
                Step::ReduceEpochLocal { reducer } => {
                    let reducer = reducer.clone_ref(py);
                    stream = stream
                        .map(lift_2tuple)
                        .accumulate(
                            HashMap::new(),
                            move |aggregators, all_key_value_in_epoch| {
                                reduce_epoch_local(&reducer, aggregators, &all_key_value_in_epoch);
                            },
                        )
                        .flat_map(|aggregators| aggregators.into_iter().map(wrap_2tuple));
                }
                Step::StatefulMap {
                    step_id,
                    builder,
                    mapper,
                } => {
                    let builder = builder.clone_ref(py);
                    let mapper = mapper.clone_ref(py);
                    stream = stream
                        .map(lift_2tuple)
                        .stateful_map(
                            recovery_store.for_step(step_id),
                            move |key| build(&builder, key),
                            move |key, state, value| stateful_map(&mapper, key, state, value),
                            hash,
                        )
                        .map(wrap_2tuple);
                }
                Step::Capture {} => {
                    let worker_output = worker_output.clone_ref(py);
                    stream
                        .inspect_time(move |epoch, item| capture(&worker_output, epoch, item))
                        .probe_with(&mut end_of_steps_probe);
                    has_capture = true;
                }
            }
        }

        if has_capture {
            let pump = Pump::new(worker_input, timely_input);
            Ok((pump, end_of_steps_probe))
        } else {
            Err("Dataflow needs to contain at least one capture".into())
        }
    })
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    Ok(())
}
