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
    reduce_epoch_local, stateful_map,
};
use crate::pyo3_extensions::{
    build, hash, lift_2tuple, wrap_2tuple, TdPyAny, TdPyCallable, TdPyIterator,
};

/// A definition of a Bytewax dataflow graph.
///
/// Use the methods defined on this class to add steps with operators
/// of the same name.
///
/// See the execution functions in the `bytewax` to run.
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
    /// It calls a function `mapper(item: Any) => updated_item: Any`
    /// on each item.
    ///
    /// It emits each updated item downstream.
    #[pyo3(text_signature = "(self, mapper)")]
    fn map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::Map { mapper });
    }

    /// Flat Map is a one-to-many transformation of items.
    ///
    /// It calls a function `mapper(item: Any) => emit: Iterable[Any]`
    /// on each item.
    ///
    /// It emits each element in the downstream iterator individually.
    #[pyo3(text_signature = "(self, mapper)")]
    fn flat_map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::FlatMap { mapper });
    }

    /// Filter selectively keeps only some items.
    ///
    /// It calls a function `predicate(item: Any) => should_emit:
    /// bool` on each item.
    ///
    /// It emits the item downstream unmodified if the predicate
    /// returns `True`.
    #[pyo3(text_signature = "(self, predicate)")]
    fn filter(&mut self, predicate: TdPyCallable) {
        self.steps.push(Step::Filter { predicate });
    }

    /// Inspect allows you to observe, but not modify, items.
    ///
    /// It calls a function `inspector(item: Any) => None` on each
    /// item.
    ///
    /// The return value is ignored; it emits items downstream
    /// unmodified.
    #[pyo3(text_signature = "(self, inspector)")]
    fn inspect(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::Inspect { inspector });
    }

    /// Inspect Epoch allows you to observe, but not modify, items and
    /// their epochs.
    ///
    /// It calls a function `inspector(epoch: int, item: Any) => None`
    /// on each item with its epoch.
    ///
    /// The return value is ignored; it emits items downstream
    /// unmodified.
    #[pyo3(text_signature = "(self, inspector)")]
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
    }

    /// Reduce lets you combine items for a key into an aggregator in
    /// epoch order.
    ///
    /// Since this is a stateful operator, it requires the the input
    /// stream has items that are `(key, value)` tuples so we can
    /// ensure that all relevant values are routed to the relevant
    /// aggregator.
    ///
    /// It calls two functions:
    ///
    /// - A `reducer(aggregator: Any, value: Any) =>
    /// updated_aggregator: Any` which combines two values. The
    /// aggregator is initially the first value seen for a key. Values
    /// will be passed in epoch order, but no order is defined within
    /// an epoch.
    ///
    /// - An `is_complete(updated_aggregator: Any) => should_emit: bool` which
    /// returns true if the most recent `(key, aggregator)` should be
    /// emitted downstream and the aggregator for that key
    /// forgotten. If there was only a single value for a key, it is
    /// passed in as the aggregator here.
    ///
    /// It emits `(key, aggregator)` tuples downstream when you tell
    /// it to.
    #[pyo3(text_signature = "(self, reducer, is_complete)")]
    fn reduce(&mut self, reducer: TdPyCallable, is_complete: TdPyCallable) {
        self.steps.push(Step::Reduce {
            reducer,
            is_complete,
        });
    }

    /// Reduce Epoch lets you combine all items for a key within an
    /// epoch into an aggregator.
    ///
    /// This is like `reduce` but marks the aggregator as complete
    /// automatically at the end of each epoch.
    ///
    /// Since this is a stateful operator, it requires the the input
    /// stream has items that are `(key, value)` tuples so we can
    /// ensure that all relevant values are routed to the relevant
    /// aggregator.
    ///
    /// It calls a function `reducer(aggregator: Any, value: Any) =>
    /// updated_aggregator: Any` which combines two values. The
    /// aggregator is initially the first value seen for a key. Values
    /// will be passed in arbitrary order.
    ///
    /// It emits `(key, aggregator)` tuples downstream at the end of
    /// each epoch.
    #[pyo3(text_signature = "(self, reducer)")]
    fn reduce_epoch(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpoch { reducer });
    }

    /// Reduce Epoch Local lets you combine all items for a key within
    /// an epoch _on a single worker._
    ///
    /// It is exactly like `reduce_epoch` but does no internal
    /// exchange between workers. You'll probably should use that
    /// instead unless you are using this as a network-overhead
    /// optimization.
    #[pyo3(text_signature = "(self, reducer)")]
    fn reduce_epoch_local(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpochLocal { reducer });
    }

    /// Stateful Map is a one-to-one transformation of values in
    /// `(key, value)` pairs, but allows you to reference a persistent
    /// state for each key when doing the transformation.
    ///
    /// Since this is a stateful operator, it requires the the input
    /// stream has items that are `(key, value)` tuples so we can
    /// ensure that all relevant values are routed to the relevant
    /// state.
    ///
    /// It calls two functions:
    ///
    /// - A `builder() => new_state: Any` which returns a new state
    /// and will be called whenever a new key is encountered.
    ///
    /// - A `mapper(state: Any, value: Any) => (updated_state: Any,
    /// updated_value: Any)` which transforms values. Values will be
    /// passed in epoch order, but no order is defined within an
    /// epoch. If the updated state is `None`, the state will be
    /// forgotten.
    ///
    /// It emits a `(key, updated_value)` tuple downstream for each
    /// input item.
    #[pyo3(text_signature = "(self, builder, mapper)")]
    fn stateful_map(&mut self, builder: TdPyCallable, mapper: TdPyCallable) {
        self.steps.push(Step::StatefulMap { builder, mapper });
    }

    /// Capture causes all `(epoch, item)` tuples that pass by this
    /// point in the Dataflow to be passed to the Dataflow's output
    /// handler.
    ///
    /// Every dataflow must contain at least one capture.
    ///
    /// If you use this operator multiple times, the results will be
    /// combined.
    ///
    /// There are no guarantees on the order that output is passed to
    /// the handler. Read the attached epoch to discern order.
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
        if let Ok(("Reduce", reducer, is_complete)) = tuple.extract() {
            return Ok(Self::Reduce {
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
        if let Ok(("StatefulMap", builder, mapper)) = tuple.extract() {
            return Ok(Self::StatefulMap { builder, mapper });
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
                reducer,
                is_complete,
            } => ("Reduce", reducer, is_complete).to_object(py),
            Self::ReduceEpoch { reducer } => ("ReduceEpoch", reducer).to_object(py),
            Self::ReduceEpochLocal { reducer } => ("ReduceEpochLocal", reducer).to_object(py),
            Self::StatefulMap { builder, mapper } => ("StatefulMap", builder, mapper).to_object(py),
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
                    reducer,
                    is_complete,
                } => {
                    let reducer = reducer.clone_ref(py);
                    let is_complete = is_complete.clone_ref(py);
                    stream = stream.map(lift_2tuple).state_machine(
                        move |key, value, aggregator: &mut Option<TdPyAny>| {
                            reduce(&reducer, &is_complete, aggregator, key, value)
                        },
                        hash,
                    );
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
                Step::StatefulMap { builder, mapper } => {
                    let builder = builder.clone_ref(py);
                    let mapper = mapper.clone_ref(py);
                    stream = stream.map(lift_2tuple).state_machine(
                        move |key, value, maybe_uninit_state: &mut Option<TdPyAny>| {
                            let state = maybe_uninit_state.get_or_insert_with(|| build(&builder));
                            stateful_map(&mapper, state, key, value)
                        },
                        hash,
                    );
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
