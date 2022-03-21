use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;

use crate::pyo3_extensions::TdPyCallable;

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
    pub steps: Vec<Step>,
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
    /// - A `builder(key: Any) => new_state: Any` which returns a
    /// new state and will be called whenever a new key is encountered
    /// with the key as a parameter.
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

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;
    Ok(())
}
