//! Newtypes around PyO3 types which allow easier interfacing with
//! Timely or other Rust libraries we use.
use crate::py_unwrap;
use crate::recovery::model::StateKey;
use crate::try_unwrap;
use crate::unwrap_any;
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::*;
use serde::ser::Error;
use std::fmt;
use std::ops::Deref;
use std::task::Poll;

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

impl ToPyObject for TdPyAny {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.0.to_object(py)
    }
}

impl IntoPy<PyObject> for TdPyAny {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0
    }
}

impl IntoPy<PyObject> for &TdPyAny {
    fn into_py(self, py: Python) -> Py<PyAny> {
        self.0.clone_ref(py)
    }
}

impl From<&PyAny> for TdPyAny {
    fn from(x: &PyAny) -> Self {
        Self(x.into())
    }
}

impl From<&PyString> for TdPyAny {
    fn from(x: &PyString) -> Self {
        Self(x.into())
    }
}

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
        Python::with_gil(|py| {
            let x = self.as_ref(py);
            let pickle = py.import("dill").map_err(S::Error::custom)?;
            let bytes = pickle
                .call_method1("dumps", (x,))
                .map_err(S::Error::custom)?
                .downcast::<PyBytes>()
                .map_err(S::Error::custom)?;
            serializer
                .serialize_bytes(bytes.as_bytes())
                .map_err(S::Error::custom)
        })
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

// Rust tests that interact with the Python interpreter don't work
// well under pyenv-virtualenv. This test executes under the global pyenv
// version, instead of the configured virtual environment.
// Disabling this test for aarch64, as it fails in CI.
#[cfg(not(target_arch = "aarch64"))]
#[test]
fn test_serde() {
    use serde_test::assert_tokens;
    use serde_test::Token;

    pyo3::prepare_freethreaded_python();

    let pyobj: TdPyAny = Python::with_gil(|py| PyString::new(py, "hello").into());

    // Python < 3.8 serializes strings differently than python >= 3.8.
    // We get the current python version here so we can assert based on that.
    let (major, minor) = Python::with_gil(|py| {
        let sys = PyModule::import(py, "sys").unwrap();
        let version = sys.getattr("version_info").unwrap();
        let major: i32 = version.getattr("major").unwrap().extract().unwrap();
        let minor: i32 = version.getattr("minor").unwrap().extract().unwrap();
        (major, minor)
    });

    // We only support python 3...
    assert_eq!(major, 3);

    let expected = if minor < 8 {
        Token::Bytes(&[128, 3, 88, 5, 0, 0, 0, 104, 101, 108, 108, 111, 113, 0, 46])
    } else {
        Token::Bytes(&[
            128, 4, 149, 9, 0, 0, 0, 0, 0, 0, 0, 140, 5, 104, 101, 108, 108, 111, 148, 46,
        ])
    };
    // This does a round-trip.
    assert_tokens(&pyobj, &[expected]);
}

/// Re-use Python's value semantics in Rust code.
impl PartialEq for TdPyAny {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            // Don't use Py.eq or PyAny.eq since it only checks
            // pointer identity.
            let self_ = self.as_ref(py);
            let other = other.as_ref(py);
            try_unwrap!(self_.rich_compare(other, CompareOp::Eq)?.is_true())
        })
    }
}

/// Turn a Python 2-tuple of `(key, value)` into a Rust 2-tuple for
/// routing into Timely's stateful operators.
pub(crate) fn extract_state_pair(key_value_pytuple: TdPyAny) -> (StateKey, TdPyAny) {
    Python::with_gil(|py| {
        let (key, value): (TdPyAny, TdPyAny) = py_unwrap!(
            key_value_pytuple.extract(py),
            format!(
                "Dataflow requires a `(key, value)` 2-tuple as input to \
                    every stateful operator for routing; got `{key_value_pytuple:?}` instead"
            )
        );
        let key: StateKey = py_unwrap!(
            key.extract(py),
            format!(
                "Stateful logic functions must return string or integer keys \
                    in `(key, value)`; got `{key:?}` instead"
            )
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

/// A Python iterator that only gets the GIL when calling [`next`] and
/// automatically wraps in [`TdPyAny`].
///
/// Otherwise the GIL would be held for the entire life of the iterator.
#[derive(Clone)]
pub(crate) struct TdPyIterator(Py<PyIterator>);

/// Have PyO3 do type checking to ensure we only make from iterable
/// objects.
impl<'source> FromPyObject<'source> for TdPyIterator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self(ob.iter()?.into()))
    }
}

impl Iterator for TdPyIterator {
    type Item = TdPyAny;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let mut iter = self.0.as_ref(py);
            iter.next().map(|r| unwrap_any!(r).into())
        })
    }
}

/// Similar to [`TdPyIterator`] but interprets the iterator sending
/// Python `None` as [`Poll::Pending`] so we can use it in a
/// coroutine-like context.
#[derive(Clone)]
pub(crate) struct TdPyCoroIterator(Py<PyIterator>);

/// Have PyO3 do type checking to ensure we only make from iterable
/// objects.
impl<'source> FromPyObject<'source> for TdPyCoroIterator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self(ob.iter()?.into()))
    }
}

// TODO: Could maybe bridge this with std::iter::AsyncIterator?
impl TdPyCoroIterator {
    pub(crate) fn next(&mut self) -> Poll<Option<TdPyAny>> {
        Python::with_gil(|py| {
            let mut iter = self.0.as_ref(py);
            match iter.next() {
                Some(Err(err)) => unwrap_any!(Err(err)),
                Some(Ok(item)) if item.is_none() => Poll::Pending,
                Some(Ok(item)) => Poll::Ready(Some(item.into())),
                None => Poll::Ready(None),
            }
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

impl IntoPy<PyObject> for TdPyCallable {
    fn into_py(self, _py: Python) -> Py<PyAny> {
        self.0
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
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Self {
        Self(py.eval("print", None, None).unwrap().into())
    }

    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    pub(crate) fn call1(&self, py: Python, args: impl IntoPy<Py<PyTuple>>) -> PyResult<Py<PyAny>> {
        self.0.call1(py, args)
    }
}
