extern crate rand;
#[macro_use(defer)]
extern crate scopeguard;

use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::ser::Error;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use timely::dataflow::operators::aggregation::*;
use timely::dataflow::operators::*;
use timely::dataflow::*;

pub(crate) mod webserver;

#[macro_use]
pub(crate) mod macros;

/// Represents a Python object flowing through a Timely dataflow.
///
/// A newtype for [`Py`]<[`PyAny`]> so we can
/// extend it with traits that Timely needs. See
/// <https://github.com/Ixrec/rust-orphan-rules> for why we need a
/// newtype and what they are.
#[derive(Clone, FromPyObject)]
struct TdPyAny(Py<PyAny>);

/// Rewrite some [`Py`] methods to automatically re-wrap as [`TdPyAny`].
impl TdPyAny {
    fn clone_ref(&self, py: Python) -> Self {
        self.0.clone_ref(py).into()
    }
}

/// Have access to all [`Py`] methods.
impl Deref for TdPyAny {
    type Target = Py<PyAny>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Allows use in some Rust to Python conversions.
// I Don't really get the difference between ToPyObject and IntoPy
// yet.
impl ToPyObject for TdPyAny {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        self.0.clone_ref(py)
    }
}

/// Allow passing to Python function calls without explicit
/// conversion.
impl IntoPy<PyObject> for TdPyAny {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0
    }
}

/// Allow passing a reference to Python function calls without
/// explicit conversion.
impl IntoPy<PyObject> for &TdPyAny {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0.clone_ref(_py)
    }
}

/// Conveniently re-wrap Python objects for use in Timely.
impl From<&PyAny> for TdPyAny {
    fn from(x: &PyAny) -> Self {
        Self(x.into())
    }
}

/// Conveniently re-wrap Python objects for use in Timely.
impl From<Py<PyAny>> for TdPyAny {
    fn from(x: Py<PyAny>) -> Self {
        Self(x)
    }
}

/// Allows you to debug print Python objects using their repr.
impl std::fmt::Debug for TdPyAny {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let self_ = self.as_ref(py);
            let repr = self_.repr()?.to_str()?;
            Ok(String::from(repr))
        });
        f.write_str(&s.map_err(|_| std::fmt::Error {})?)
    }
}

/// Serialize Python objects flowing through Timely that cross
/// process bounds as pickled bytes.
impl serde::Serialize for TdPyAny {
    // We can't do better than isolating the Result<_, PyErr> part and
    // the explicitly converting.  1. `?` automatically trys to
    // convert using From<ReturnedError> for OuterError. But orphan
    // rule means we can't implement it since we don't own either py
    // or serde error types.  2. Using the newtype trick isn't worth
    // it, since you'd have to either wrap all the PyErr when they're
    // generated, or you implement From twice, once to get MyPyErr and
    // once to get serde::Err. And then you're calling .into()
    // explicitly since the last line isn't a `?` anyway.
    //
    // There's the separate problem if we could even implement the
    // Froms. We aren't allowed to "capture" generic types in an inner
    // `impl<S>` https://doc.rust-lang.org/error-index.html#E0401 and
    // we can't move them top-level since we don't know the concrete
    // type of S, and you're not allowed to have unconstrained generic
    // parameters https://doc.rust-lang.org/error-index.html#E0207 .
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes: Result<Vec<u8>, PyErr> = Python::with_gil(|py| {
            let x = self.as_ref(py);
            let pickle = py.import("pickle")?;
            let bytes = pickle.call_method1("dumps", (x,))?.extract()?;
            Ok(bytes)
        });
        serializer.serialize_bytes(bytes.map_err(S::Error::custom)?.as_slice())
    }
}

struct PickleVisitor;
impl<'de> serde::de::Visitor<'de> for PickleVisitor {
    type Value = TdPyAny;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a pickled byte array")
    }

    fn visit_bytes<E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let x: Result<TdPyAny, PyErr> = Python::with_gil(|py| {
            let pickle = py.import("pickle")?;
            let x = pickle.call_method1("loads", (bytes,))?.into();
            Ok(x)
        });
        x.map_err(E::custom)
    }
}

/// Deserialize Python objects flowing through Timely that cross
/// process bounds from pickled bytes.
impl<'de> serde::Deserialize<'de> for TdPyAny {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(PickleVisitor)
    }
}

/// Re-use Python's value semantics in Rust code.
///
/// Timely requires this whenever it internally "groups by".
impl PartialEq for TdPyAny {
    fn eq(&self, other: &Self) -> bool {
        use pyo3::class::basic::CompareOp;

        Python::with_gil(|py| {
            // Don't use Py.eq or PyAny.eq since it only checks
            // pointer identity.
            let self_ = self.as_ref(py);
            let other = other.as_ref(py);
            with_traceback!(py, self_.rich_compare(other, CompareOp::Eq)?.is_true())
        })
    }
}

/// Possibly a footgun.
///
/// Timely internally stores values in [`HashMap`] which require keys
/// to be [`Eq`] so we have to implement this (or somehow write our
/// own hash maps that could ignore this), but we can't actually
/// guarantee that any Python type will actually have a correct sense
/// of total equality. It's possible broken `__eq__` implementations
/// will cause mysterious behavior.
impl Eq for TdPyAny {}

/// Custom hashing semantics.
///
/// We can't use Python's `hash` because it is not consistent across
/// processes. Instead we have our own "exchange hash". See module
/// `bytewax.exhash` for definitions and how to implement for your own
/// types.
///
/// Timely requires this whenever it exchanges or accumulates data in
/// hash maps.
impl Hash for TdPyAny {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Python::with_gil(|py| {
            with_traceback!(py, {
                let exhash = PyModule::import(py, "bytewax.exhash")?;
                let digest = exhash
                    .getattr("exhash")?
                    .call1((self,))?
                    .call_method0("digest")?
                    .extract()?;
                state.write(digest);
                PyResult::Ok(())
            });
        });
    }
}

/// A Python iterator that only gets the GIL when calling .next() and
/// automatically wraps in [`TdPyAny`].
///
/// Otherwise the GIL would be held for the entire life of the iterator.
#[derive(Clone)]
struct TdPyIterator(Py<PyIterator>);

/// Have PyO3 do type checking to ensure we only make from iterable
/// objects.
impl<'source> FromPyObject<'source> for TdPyIterator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.iter()?.into())
    }
}

/// Conveniently re-wrap PyO3 objects.
impl From<&PyIterator> for TdPyIterator {
    fn from(x: &PyIterator) -> Self {
        Self(x.into())
    }
}

impl Iterator for TdPyIterator {
    type Item = TdPyAny;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let mut iter = self.0.as_ref(py);
            iter.next().map(|r| with_traceback!(py, r).into())
        })
    }
}

/// A Python object that is callable.
struct TdPyCallable(Py<PyAny>);

/// Have PyO3 do type checking to ensure we only make from callable
/// objects.
impl<'source> FromPyObject<'source> for TdPyCallable {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.is_callable() {
            Ok(Self(ob.into()))
        } else {
            let msg = if let Ok(type_name) = ob.get_type().name() {
                format!("'{type_name}' object is not callable")
            } else {
                "object is not callable".to_string()
            };
            Err(PyTypeError::new_err(msg))
        }
    }
}

impl ToPyObject for TdPyCallable {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        self.0.clone_ref(py)
    }
}

/// Restricted Rust interface that only makes sense on callable
/// objects.
///
/// Just pass through to [`Py`].
impl TdPyCallable {
    fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    fn call0(&self, py: Python) -> PyResult<Py<PyAny>> {
        self.0.call0(py)
    }

    fn call1(&self, py: Python, args: impl IntoPy<Py<PyTuple>>) -> PyResult<Py<PyAny>> {
        self.0.call1(py, args)
    }
}

/// The definition of one step in a Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// See
/// <https://docs.rs/timely/latest/timely/dataflow/operators/index.html>
/// for Timely's operators. We try to keep the same semantics here.
enum Step {
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

/// The definition of Bytewax dataflow graph.
///
/// This isn't actually used during execution, just during building.
///
/// TODO: Right now this is just a linear dataflow only.
#[pyclass(module = "bytewax")] // Required to support pickling.
struct Dataflow {
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

    /// **Map** is a one-to-one transformation of items.
    ///
    /// It calls a function `mapper(item: Any) => updated_item: Any`
    /// on each item.
    ///
    /// It emits each transformed item downstream.
    fn map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::Map { mapper });
    }

    /// **Flat Map** is a one-to-many transformation of items.
    ///
    /// It calls a function `mapper(item: Any) => emit: Iterable[Any]`
    /// on each item.
    ///
    /// It emits each element in the downstream iterator individually.
    fn flat_map(&mut self, mapper: TdPyCallable) {
        self.steps.push(Step::FlatMap { mapper });
    }

    /// **Filter** selectively keeps only some items.
    ///
    /// It calls a function `predicate(item: Any) => should_emit:
    /// bool` on each item.
    ///
    /// It emits the item downstream unmodified if the predicate
    /// returns `True`.
    fn filter(&mut self, predicate: TdPyCallable) {
        self.steps.push(Step::Filter { predicate });
    }

    /// **Inspect** allows you to observe, but not modify, items.
    ///
    /// It calls a function `inspector(item: Any) => None` on each
    /// item.
    ///
    /// The return value is ignored; it emits items downstream
    /// unmodified.
    fn inspect(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::Inspect { inspector });
    }

    /// **Inspect Epoch** allows you to observe, but not modify, items
    /// and their epochs.
    ///
    /// It calls a function `inspector(epoch: int, item: Any) => None`
    /// on each item with its epoch.
    ///
    /// The return value is ignored; it emits items downstream
    /// unmodified.
    fn inspect_epoch(&mut self, inspector: TdPyCallable) {
        self.steps.push(Step::InspectEpoch { inspector });
    }

    /// **Reduce** lets you combine items for a key into an
    /// aggregator in epoch order.
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
    fn reduce(&mut self, reducer: TdPyCallable, is_complete: TdPyCallable) {
        self.steps.push(Step::Reduce {
            reducer,
            is_complete,
        });
    }

    /// **Reduce Epoch** lets you combine all items for a key within
    /// an epoch into an aggregator.
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
    fn reduce_epoch(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpoch { reducer });
    }

    /// **Reduce Epoch Local** lets you combine all items for a key
    /// within an epoch _on a single worker._
    ///
    /// It is exactly like `reduce_epoch` but does no internal
    /// exchange between workers. You'll probably should use that
    /// instead unless you are using this as a network-overhead
    /// optimization.
    fn reduce_epoch_local(&mut self, reducer: TdPyCallable) {
        self.steps.push(Step::ReduceEpochLocal { reducer });
    }

    /// **Stateful Map** is a one-to-one transformation of values in
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
    fn stateful_map(&mut self, builder: TdPyCallable, mapper: TdPyCallable) {
        self.steps.push(Step::StatefulMap { builder, mapper });
    }

    /// **Capture** causes all `(epoch, item)` tuples that pass by
    /// this point in the Dataflow to be passed to the Dataflow's
    /// output handler.
    ///
    /// If you use this operator multiple times, the results will be
    /// combined.
    ///
    /// There are no guarantees on the order that output is passed to
    /// the handler. Read the attached epoch to discern order.
    fn capture(&mut self) {
        self.steps.push(Step::Capture {});
    }
}

// These are all shims which map the Timely Rust API into equivalent
// calls to Python functions through PyO3.

fn build(builder: &TdPyCallable) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, builder.call0(py)).into())
}

fn map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))).into())
}

fn flat_map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyIterator {
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))?.extract(py)))
}

fn filter(predicate: &TdPyCallable, item: &TdPyAny) -> bool {
    Python::with_gil(|py| with_traceback!(py, predicate.call1(py, (item,))?.extract(py)))
}

fn inspect(inspector: &TdPyCallable, item: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (item,)));
    });
}

fn inspect_epoch(inspector: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (*epoch, item)));
    });
}

/// Turn a Python 2-tuple into a Rust 2-tuple.
fn lift_2tuple(key_value_pytuple: TdPyAny) -> (TdPyAny, TdPyAny) {
    Python::with_gil(|py| with_traceback!(py, key_value_pytuple.as_ref(py).extract()))
}

/// Turn a Rust 2-tuple into a Python 2-tuple.
fn wrap_2tuple(key_value: (TdPyAny, TdPyAny)) -> TdPyAny {
    Python::with_gil(|py| key_value.to_object(py).into())
}

fn hash(key: &TdPyAny) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn reduce(
    reducer: &TdPyCallable,
    is_complete: &TdPyCallable,
    aggregator: &mut Option<TdPyAny>,
    key: &TdPyAny,
    value: TdPyAny,
) -> (bool, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let updated_aggregator = match aggregator {
            Some(aggregator) => {
                with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value))).into()
            }
            None => value,
        };
        let should_emit_and_discard_aggregator: bool = with_traceback!(
            py,
            is_complete
                .call1(py, (updated_aggregator.clone_ref(py),))?
                .extract(py)
        );

        *aggregator = Some(updated_aggregator.clone_ref(py));

        if should_emit_and_discard_aggregator {
            let emit = (key.clone_ref(py), updated_aggregator).to_object(py).into();
            (true, Some(emit))
        } else {
            (false, None)
        }
    })
}

fn reduce_epoch(
    reducer: &TdPyCallable,
    aggregator: &mut Option<TdPyAny>,
    _key: &TdPyAny,
    value: TdPyAny,
) {
    Python::with_gil(|py| {
        let updated_aggregator = match aggregator {
            Some(aggregator) => {
                with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value))).into()
            }
            None => value,
        };
        *aggregator = Some(updated_aggregator);
    });
}

fn reduce_epoch_local(
    reducer: &TdPyCallable,
    aggregators: &mut HashMap<TdPyAny, TdPyAny>,
    all_key_value_in_epoch: &Vec<(TdPyAny, TdPyAny)>,
) {
    Python::with_gil(|py| {
        for (key, value) in all_key_value_in_epoch {
            aggregators
                .entry(key.clone_ref(py))
                .and_modify(|aggregator| {
                    *aggregator =
                        with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value)))
                            .into()
                })
                .or_insert(value.clone_ref(py));
        }
    });
}

fn stateful_map(
    mapper: &TdPyCallable,
    state: &mut TdPyAny,
    key: &TdPyAny,
    value: TdPyAny,
) -> (bool, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let (updated_state, emit_value): (TdPyAny, TdPyAny) = with_traceback!(
            py,
            mapper.call1(py, (state.clone_ref(py), value))?.extract(py)
        );

        *state = updated_state;

        let discard_state = Python::with_gil(|py| state.is_none(py));
        let emit = (key, emit_value).to_object(py).into();
        (discard_state, std::iter::once(emit))
    })
}

fn capture(captor: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| with_traceback!(py, captor.call1(py, ((*epoch, item.clone_ref(py)),))));
}

// End of shim functions.

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
///
/// This will be called in the worker's "main" loop to feed data in.
struct Pump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl Pump {
    fn new(pull_from_pyiter: TdPyIterator, push_to_timely: InputHandle<u64, TdPyAny>) -> Self {
        Self {
            pull_from_pyiter,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }

    /// Take a single data element and timestamp and feed it into the
    /// dataflow.
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(epoch_item_pytuple) = pull_from_pyiter.next() {
                let (epoch, item) = with_traceback!(py, epoch_item_pytuple?.extract());
                self.push_to_timely.advance_to(epoch);
                self.push_to_timely.send(item);
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_remains(&self) -> bool {
        !self.pyiter_is_empty
    }
}

fn build_dataflow<A>(
    timely_worker: &mut timely::worker::Worker<A>,
    py: Python,
    flow: &Dataflow,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
) -> (Pump, ProbeHandle<u64>)
where
    A: timely::communication::Allocate,
{
    let worker_index = timely_worker.index();
    let worker_count = timely_worker.peers();
    timely_worker.dataflow(|scope| {
        let mut timely_input = InputHandle::new();
        let mut end_of_steps_probe = ProbeHandle::new();
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
                    stream.inspect_time(move |epoch, item| capture(&worker_output, epoch, item));
                }
            }
        }
        stream.probe_with(&mut end_of_steps_probe);

        let pump = Pump::new(worker_input, timely_input);
        (pump, end_of_steps_probe)
    })
}

/// "Main loop" of a Timely worker thread.
///
/// 1. Pump [`Pump`]s (get input data from Python into Timely).
///
/// 2. Dispose of empty [`Pump`]s and Probes which indicate there's no
/// data left to process in that dataflow.
///
/// 3. Call [timely::worker::Worker::step] to tell Timely to do
/// whatever work it can.
fn worker_main<A>(
    mut pumps_with_input_remaining: Vec<Pump>,
    mut probes_with_timestamps_inflight: Vec<ProbeHandle<u64>>,
    interrupt_flag: &AtomicBool,
    worker: &mut timely::worker::Worker<A>,
) where
    A: timely::communication::Allocate,
{
    while (!pumps_with_input_remaining.is_empty() || !probes_with_timestamps_inflight.is_empty())
        && !interrupt_flag.load(Ordering::Relaxed)
    {
        if !pumps_with_input_remaining.is_empty() {
            let mut updated_pumps = Vec::new();
            for mut pump in pumps_with_input_remaining.into_iter() {
                pump.pump();
                if pump.input_remains() {
                    updated_pumps.push(pump);
                }
            }
            pumps_with_input_remaining = updated_pumps;
        }
        if !probes_with_timestamps_inflight.is_empty() {
            let mut updated_probes = Vec::new();
            for probe in probes_with_timestamps_inflight.into_iter() {
                if !probe.done() {
                    updated_probes.push(probe);
                }
            }
            probes_with_timestamps_inflight = updated_probes;
        }

        worker.step();
    }
}

fn shutdown_worker<A>(worker: &mut timely::worker::Worker<A>)
where
    A: timely::communication::Allocate,
{
    for dataflow_id in worker.installed_dataflows() {
        worker.drop_dataflow(dataflow_id);
    }
}

/// Execute a dataflow in the current thread.
///
/// Blocks until execution is complete.
///
/// See `run_sync()` for a convenience method to pass data through a
/// dataflow for notebook development.
///
/// >>> flow = Dataflow()
/// >>> def input_builder(_, _):
/// ...     return enumerate(range(3))
/// >>> def output_builder(_, _):
/// ...     return print
/// >>> main_sync(flow, input_builder, output_builder)
///
/// Args:
///     flow: Dataflow to run.
///     input_builder: Returns all input to be processed.
///     output_builder: Returns a function which will be called with
///         each `(epoch, item)` that passes by a capture operator.
#[pyfunction]
fn main_sync(
    py: Python,
    flow: Py<Dataflow>,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
) -> PyResult<()> {
    let result = py.allow_threads(move || {
        std::panic::catch_unwind(|| {
            timely::execute::execute_directly(move |worker| {
                let (pump, probe) = Python::with_gil(|py| {
                    let flow = &flow.as_ref(py).borrow();
                    build_dataflow(worker, py, flow, input_builder, output_builder)
                });

                worker_main(vec![pump], vec![probe], &AtomicBool::new(false), worker);

                shutdown_worker(worker);
            });
        })
    });

    result.map_err(|box_err| {
        if let Some(pyerr) = box_err.downcast_ref::<PyErr>() {
            pyerr.clone_ref(py)
        } else {
            PyRuntimeError::new_err("Panic in Rust code")
        }
    })
}

/// Execute a dataflow in the current process as part of a cluster.
///
/// You have to coordinate starting up all the processes in the
/// cluster and ensuring they each are assigned a unique ID and know
/// the addresses of other processes. You'd commonly use this for
/// starting processes as part of a Kubernetes cluster.
///
/// Blocks until execution is complete.
///
/// See `run_cluster()` for a convenience method to pass data through
/// a dataflow for notebook development.
///
/// See `main_cluster()` for starting a simple cluster locally on one
/// machine.
///
/// >>> flow = Dataflow()
/// >>> def input_builder(worker_index, worker_count):
/// ...     return enumerate(range(3))
/// >>> def output_builder(worker_index, worker_count):
/// ...     return print
/// >>> main_proc(flow, input_builder, output_builder)
///
/// Args:
///     flow: Dataflow to run.
///     input_builder: Returns input that each worker thread should
///         process.
///     output_builder: Returns a callback function for each worker
///         thread, called with `(epoch, item)` whenever and item
///         passes by a capture operator on this process.
///     addresses: List of host/port addresses for all processes in
///         this cluster (including this one).
///     proc_id: Index of this process in cluster; starts from 0.
#[pyfunction]
fn main_proc(
    py: Python,
    flow: Py<Dataflow>,
    input_builder: TdPyCallable,
    output_builder: TdPyCallable,
    addresses: Option<Vec<String>>,
    proc_id: usize,
    worker_count_per_proc: usize,
) -> PyResult<()> {
    py.allow_threads(move || {
        let addresses = addresses.unwrap_or_default();
        let (builders, other) = if addresses.len() < 1 {
            timely::CommunicationConfig::Process(worker_count_per_proc)
        } else {
            timely::CommunicationConfig::Cluster {
                threads: worker_count_per_proc,
                process: proc_id,
                addresses,
                report: false,
                log_fn: Box::new(|_| None),
            }
        }
        .try_build()
        .map_err(PyRuntimeError::new_err)?;

        let should_shutdown = Arc::new(AtomicBool::new(false));
        let should_shutdown_w = should_shutdown.clone();
        let should_shutdown_p = should_shutdown.clone();
        // We can drop these if we want to use nightly
        // Thread::is_running
        // https://github.com/rust-lang/rust/issues/90470
        let shutdown_worker_count = Arc::new(AtomicUsize::new(0));
        let shutdown_worker_count_w = shutdown_worker_count.clone();
        let shutdown_worker_count_p = shutdown_worker_count.clone();

        // Panic hook is per-process, so this isn't perfect as you
        // can't call Executor.build_and_run() concurrently.
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            should_shutdown_p.store(true, Ordering::Relaxed);
            shutdown_worker_count_p.fetch_add(1, Ordering::Relaxed);

            if let Some(pyerr) = info.payload().downcast_ref::<PyErr>() {
                Python::with_gil(|py| pyerr.print(py));
            } else {
                default_hook(info);
            }
        }));
        // Don't chain panic hooks if we run multiple
        // dataflows. Really this is all a hack because the panic
        // hook is global state. There's some talk of per-thread
        // panic hooks which would help
        // here. https://internals.rust-lang.org/t/pre-rfc-should-std-set-hook-have-a-per-thread-version/9518/3
        defer! {
            let _ = std::panic::take_hook();
        }

        let guards = timely::execute::execute_from(
            builders,
            other,
            timely::WorkerConfig::default(),
            move |worker| {
                let (pump, probe) = Python::with_gil(|py| {
                    let flow = &flow.as_ref(py).borrow();
                    let input_builder = input_builder.clone_ref(py);
                    let output_builder = output_builder.clone_ref(py);
                    build_dataflow(worker, py, flow, input_builder, output_builder)
                });

                worker_main(vec![pump], vec![probe], &should_shutdown_w, worker);

                shutdown_worker(worker);
                shutdown_worker_count_w.fetch_add(1, Ordering::Relaxed);
            },
        )
        .map_err(PyRuntimeError::new_err)?;

        // Recreating what Python does in Thread.join() to "block"
        // but also check interrupt handlers.
        // https://github.com/python/cpython/blob/204946986feee7bc80b233350377d24d20fcb1b8/Modules/_threadmodule.c#L81
        let workers_in_proc_count = guards.guards().len();
        while shutdown_worker_count.load(Ordering::Relaxed) < workers_in_proc_count {
            thread::sleep(Duration::from_millis(1));
            Python::with_gil(|py| Python::check_signals(py)).map_err(|err| {
                should_shutdown.store(true, Ordering::Relaxed);
                err
            })?;
        }
        for maybe_worker_panic in guards.join() {
            // See if we can PR Timely to not cast panic info to
            // String. Then we could re-raise Python exception in
            // main thread and not need to print in
            // panic::set_hook above, although we still need it to
            // tell the other workers to do graceful shutdown.
            maybe_worker_panic.map_err(|_| {
                PyRuntimeError::new_err("Worker thread died; look for errors above")
            })?;
        }

        Ok(())
    })
}

#[pyfunction]
fn sleep_keep_gil(secs: u64) {
    thread::sleep(Duration::from_secs(secs));
}

#[pyfunction]
fn sleep_release_gil(py: Python, secs: u64) {
    py.allow_threads(|| {
        thread::sleep(Duration::from_secs(secs));
    });
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_tiny_dancer(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Dataflow>()?;

    m.add_function(wrap_pyfunction!(main_sync, m)?)?;
    m.add_function(wrap_pyfunction!(main_proc, m)?)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
