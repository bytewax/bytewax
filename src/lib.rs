#[macro_use(defer)]
extern crate scopeguard;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::ser::Error;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Deref;
use std::thread;
use std::time::Duration;

use timely::dataflow::*;

pub(crate) mod dataflow;
pub(crate) mod execution;
pub(crate) mod operators;
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
            let pickle = py.import("dill")?;
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
            let pickle = py.import("dill")?;
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
pub(crate) struct TdPyCallable(Py<PyAny>);

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

fn build(builder: &TdPyCallable) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, builder.call0(py)).into())
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

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
///
/// This will be called in the worker's "main" loop to feed data in.
pub(crate) struct Pump {
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

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_tiny_dancer(_py: Python, m: &PyModule) -> PyResult<()> {
    execution::register(_py, m)?;
    dataflow::register(_py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
