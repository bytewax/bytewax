//! Serialization of Python object for recovery and transport.

use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use pyo3::types::PyType;

use crate::pyo3_extensions::TdPyAny;

/// Represents a `bytewax.serde.Serde` from Python.
#[derive(Clone)]
pub(crate) struct Serde(Py<PyAny>);

static SERDE_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

fn get_serde_module(py: Python) -> PyResult<&Bound<'_, PyModule>> {
    Ok(SERDE_MODULE
        .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
            Ok(py.import_bound("bytewax.serde")?.into())
        })?
        .bind(py))
}

static SERDE_ABC: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn get_serde_abc(py: Python) -> PyResult<&Bound<'_, PyAny>> {
    Ok(SERDE_ABC
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(get_serde_module(py)?.getattr("Serde")?.into())
        })?
        .bind(py))
}

/// Do some eager type checking.
impl<'py> FromPyObject<'py> for Serde {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        let ob: &PyType = obj.extract()?;
        // We use [`PyType::is_subclass`] rather than
        // [`PyAny::is_instance`] because all the ABC methods are
        // `@staticmethod` so we call them on the class itself rather
        // than an instance.
        if !ob.is_subclass(get_serde_abc(py)?.as_gil_ref())? {
            Err(PyTypeError::new_err(
                "serialization must subclass `bytewax.serde.Serde`",
            ))
        } else {
            Ok(Self(ob.into()))
        }
    }
}

impl IntoPy<Py<PyAny>> for Serde {
    fn into_py(self, _py: Python<'_>) -> Py<PyAny> {
        self.0
    }
}

impl Serde {
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    pub(crate) fn ser(&self, py: Python, state: PyObject) -> PyResult<Vec<u8>> {
        self.0
            .call_method1(py, intern!(py, "ser"), (state,))?
            .extract(py)
    }

    pub(crate) fn de(&self, py: Python, s: Vec<u8>) -> PyResult<TdPyAny> {
        Ok(self
            .0
            .call_method1(py, intern!(py, "de"), (PyBytes::new_bound(py, &s),))?
            .into())
    }
}
