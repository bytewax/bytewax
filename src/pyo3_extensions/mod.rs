use pyo3::basic::CompareOp;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::ser::Error;
use serde::Deserialize;
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use crate::with_traceback;

/// Represents a Python object flowing through a Timely dataflow.
///
/// A newtype for [`Py`]<[`PyAny`]> so we can
/// extend it with traits that Timely needs. See
/// <https://github.com/Ixrec/rust-orphan-rules> for why we need a
/// newtype and what they are.
#[derive(Clone, FromPyObject)]
pub(crate) struct TdPyAny(Py<PyAny>);

/// Rewrite some [`Py`] methods to automatically re-wrap as [`TdPyAny`].
impl TdPyAny {
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
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
impl From<&PyString> for TdPyAny {
    fn from(x: &PyString) -> Self {
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

impl fmt::Debug for TdPyCallable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let self_ = self.0.as_ref(py);
            let name: String = self_.getattr("__name__")?.extract()?;
            Ok(name)
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

pub(crate) struct PickleVisitor;

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

#[test]
fn test_serde() {
    use serde_test::assert_tokens;
    use serde_test::Token;

    pyo3::prepare_freethreaded_python();

    let pyobj: TdPyAny = Python::with_gil(|py| PyString::new(py, "hello").into());
    // This does a round-trip.
    assert_tokens(
        &pyobj,
        &[Token::Bytes(&[
            128, 4, 149, 9, 0, 0, 0, 0, 0, 0, 0, 140, 5, 104, 101, 108, 108, 111, 148, 46,
        ])],
    );
}

/// Re-use Python's value semantics in Rust code.
impl PartialEq for TdPyAny {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            // Don't use Py.eq or PyAny.eq since it only checks
            // pointer identity.
            let self_ = self.as_ref(py);
            let other = other.as_ref(py);
            with_traceback!(py, self_.rich_compare(other, CompareOp::Eq)?.is_true())
        })
    }
}

/// Newtype over a string representing the "routing key" for stateful
/// operators.
///
/// We use a string here rather than wrapping any Python type because
/// the routing key interfaces with a lot of Bytewax and Timely code
/// which puts requirements on it: it has to be hashable, have
/// equality, debug printable, and is serde-able and we can't
/// guarantee those things are correct on any arbitrary Python type.
///
/// Yes, we lose a little bit of flexibility, but it makes usage more
/// convenient.
///
/// You'll mostly interface with this via [`extract_state_pair`] and
/// [`wrap_state_pair`].
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, FromPyObject,
)]
pub(crate) struct StateKey(String);

impl StateKey {
    pub(crate) fn new(s: String) -> Self {
        Self(s)
    }

    /// Hash this key for Timely exchange routing.
    pub(crate) fn route(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<StateKey> for String {
    fn from(key: StateKey) -> Self {
        key.0
    }
}

impl IntoPy<PyObject> for StateKey {
    fn into_py(self, py: Python) -> Py<PyAny> {
        PyString::new(py, &self.0).into()
    }
}

impl ToPyObject for StateKey {
    fn to_object(&self, py: Python) -> Py<PyAny> {
        self.0.to_object(py)
    }
}

/// Turn a Python 2-tuple of `(key, value)` into a Rust 2-tuple for
/// routing into Timely's stateful operators.
pub(crate) fn extract_state_pair(key_value_pytuple: TdPyAny) -> (StateKey, TdPyAny) {
    Python::with_gil(|py| {
        let (key, value): (TdPyAny, TdPyAny) = with_traceback!(py, key_value_pytuple.extract(py)
            .map_err(|_err| PyTypeError::new_err(format!("Dataflow requires a `(key, value)` 2-tuple as input to every stateful operator; got `{key_value_pytuple:?}` instead"))));
        let key: StateKey = with_traceback!(
            py,
            key.extract(py).map_err(|_err| PyTypeError::new_err(format!(
                "Stateful operators require string keys in `(key, value)`; got `{key:?}` instead"
            )))
        );
        (key, value)
    })
}

/// Turn a Rust 2-tuple of `(key, value)` into a Python 2-tuple.
pub(crate) fn wrap_state_pair(key_value: (StateKey, TdPyAny)) -> TdPyAny {
    Python::with_gil(|py| {
        let key_value_pytuple: Py<PyAny> = key_value.into_py(py);
        key_value_pytuple.into()
    })
}

/// A Python iterator that only gets the GIL when calling .next() and
/// automatically wraps in [`TdPyAny`].
///
/// Otherwise the GIL would be held for the entire life of the iterator.
#[derive(Clone)]
pub(crate) struct TdPyIterator(pub(crate) Py<PyIterator>);

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
#[derive(Clone)]
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
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    pub(crate) fn call1(&self, py: Python, args: impl IntoPy<Py<PyTuple>>) -> PyResult<Py<PyAny>> {
        self.0.call1(py, args)
    }
}

impl IntoPy<PyObject> for TdPyCallable {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0
    }
}
