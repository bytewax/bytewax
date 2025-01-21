//! Newtypes around PyO3 types which allow easier interfacing with
//! Timely or other Rust libraries we use.
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use serde::ser::Error;
use std::fmt;
use std::sync::Arc;

static PICKLE_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

/// Represents a Python object flowing through a Timely dataflow.
///
/// As soon as you need to manipulate this object, convert it into a
/// [`PyObject`] or bind it into a [`Bound`]. This should only exist
/// within the dataflow.
///
/// A newtype for [`Py`]`<`[`PyAny`]`>` so we can extend it with
/// traits that Timely needs. See
/// <https://github.com/Ixrec/rust-orphan-rules> for why we need a
/// newtype and what they are.
///
/// This needs to be [`Arc`]`<`[`PyObject`]`>` because [`PyObject`] is
/// not [`Clone`] because it's not possible to arbitrarily clone
/// without holding the GIL. We expose an API such that we must have
/// the GIL to convert to [`PyObject`] for use in logic calls which
/// "collapses" all of the outstanding [`Arc`] references back to a
/// single [`PyObject`] reference while we have the GIL.
///
/// This can still cause memory leaks (which is technically not
/// unsoundness) unless we have the GIL during `drop`. To avoid this
/// with this current API, every [`TdPyAny`] instance should "end"
/// with `into_py`, but we currently can't do that because Timely
/// serializes and drops the value on exchange. See
/// https://pyo3.rs/v0.23.4/migration.html#pyclone-is-now-gated-behind-the-py-clone-feature
/// for more info.
#[derive(Clone)]
pub(crate) struct TdPyAny(Arc<PyObject>);

impl TdPyAny {
    pub(crate) fn bind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.0.bind(py)
    }

    pub(crate) fn into_py(self, py: Python<'_>) -> PyObject {
        match Arc::try_unwrap(self.0) {
            Ok(x) => x,
            Err(self_) => self_.clone_ref(py),
        }
    }
}

impl From<PyObject> for TdPyAny {
    fn from(x: PyObject) -> Self {
        Self(Arc::new(x))
    }
}

impl<'py, T> From<Bound<'py, T>> for TdPyAny {
    fn from(x: Bound<'py, T>) -> Self {
        Self(Arc::new(x.into_any().unbind()))
    }
}

/// Allows you to debug print Python objects using their repr.
impl std::fmt::Debug for TdPyAny {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let self_ = self.bind(py);
            let binding = self_.repr()?;
            let repr = binding.to_str()?;
            Ok(String::from(repr))
        });
        f.write_str(&s.map_err(|_| std::fmt::Error {})?)
    }
}

/// Serialize Python objects flowing through Timely that cross
/// process bounds as bytes.
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
            let x = self.bind(py);
            let pickle = PICKLE_MODULE
                .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
                    Ok(py.import("pickle")?.unbind())
                })
                .map_err(S::Error::custom)?;
            let binding = pickle
                .bind(py)
                .call_method1(intern!(py, "dumps"), (x,))
                .map_err(S::Error::custom)?;
            let bytes = binding.downcast::<PyBytes>().map_err(S::Error::custom)?;
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

    fn visit_bytes<'py, E>(self, bytes: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let x: Result<TdPyAny, PyErr> = Python::with_gil(|py| {
            let pickle = py.import("pickle")?;
            let x = pickle
                .call_method1(intern!(py, "loads"), (bytes,))?
                .unbind()
                .into();
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

/// A Python object that is callable.
///
/// To actually call, you must [`bind`] it and use the bound interface
/// in order to not need to have a dual `TdPyX` vs `TdBoundX`.
pub(crate) struct TdPyCallable(PyObject);

/// Have PyO3 do type checking to ensure we only make from callable
/// objects.
impl<'py> FromPyObject<'py> for TdPyCallable {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_callable() {
            let py = ob.py();
            Ok(Self(ob.as_unbound().clone_ref(py)))
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

impl fmt::Debug for TdPyCallable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s: PyResult<String> = Python::with_gil(|py| {
            let name: String = self.0.bind(py).getattr("__name__")?.extract()?;
            Ok(name)
        });
        f.write_str(&s.map_err(|_| std::fmt::Error {})?)
    }
}

impl TdPyCallable {
    pub(crate) fn bind<'py>(&self, py: Python<'py>) -> &Bound<'py, PyAny> {
        self.0.bind(py)
    }
}

// This is a trait that can be implemented by any parent class.
// The function returns one of the possible subclasses instances.
pub(crate) trait PyConfigClass<S> {
    fn downcast(&self, py: Python) -> PyResult<S>;
}

pub(crate) trait OptionPyExt {
    fn cloned_ref(&self, py: Python) -> Self;
}

impl<T> OptionPyExt for Option<Py<T>> {
    fn cloned_ref(&self, py: Python) -> Self {
        self.as_ref().map(|x| x.clone_ref(py))
    }
}
