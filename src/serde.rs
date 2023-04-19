//!

use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyType;

use crate::pyo3_extensions::TdPyAny;
use crate::unwrap_any;

/// Represents a `bytewax.serde.Serde` from Python.
#[derive(Clone)]
pub(crate) struct Serde(Py<PyAny>);

static SERDE_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

fn get_serde_module(py: Python) -> PyResult<&PyModule> {
    Ok(SERDE_MODULE
        .get_or_try_init(py, || -> PyResult<Py<PyModule>> {
            Ok(py.import("bytewax.serde")?.into())
        })?
        .as_ref(py))
}

static SERDE_ABC: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn get_serde_abc(py: Python) -> PyResult<&PyAny> {
    Ok(SERDE_ABC
        .get_or_try_init(py, || -> PyResult<Py<PyAny>> {
            Ok(get_serde_module(py)?.getattr("Serde")?.into())
        })?
        .as_ref(py))
}

static SERDE_JP: GILOnceCell<Serde> = GILOnceCell::new();

fn get_serde_jp(py: Python) -> PyResult<Serde> {
    Ok(SERDE_JP
        .get_or_try_init(py, || -> PyResult<Serde> {
            get_serde_module(py)?.getattr("JsonPickleSerde")?.extract()
        })?
        .clone())
}

/// Do some eager type checking.
impl<'source> FromPyObject<'source> for Serde {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let py = ob.py();
        let ob: &PyType = ob.extract()?;
        // We use [`PyType::is_subclass`] rather than
        // [`PyAny::is_instance`] because all the ABC methods are
        // `@staticmethod` so we call them on the class itself rather
        // than an instance.
        if !ob.is_subclass(get_serde_abc(py)?)? {
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

impl Default for Serde {
    fn default() -> Self {
        unwrap_any!(Python::with_gil(|py| get_serde_jp(py)))
    }
}

impl Serde {
    pub(crate) fn clone_ref(&self, py: Python) -> Self {
        Self(self.0.clone_ref(py))
    }

    ///
    pub(crate) fn ser(&self, py: Python, state: TdPyAny) -> PyResult<String> {
        self.0
            .call_method1(py, intern!(py, "ser"), (state,))?
            .extract(py)
    }

    ///
    pub(crate) fn de(&self, py: Python, s: String) -> PyResult<TdPyAny> {
        Ok(self.0.call_method1(py, intern!(py, "de"), (s,))?.into())
    }
}
