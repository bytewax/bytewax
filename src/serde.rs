//! Serialization of Python object for recovery and transport.

use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;

// This needed to be mutable in order to swap it's value when testing.
static SERDE_OBJ: GILOnceCell<PyObject> = GILOnceCell::new();

/// Load, or initialize a Python object to use for Serde
/// into a GILOnceCell.
pub(crate) fn get_serde_obj(py: Python) -> PyResult<&Bound<'_, PyAny>> {
    Ok(SERDE_OBJ
        .get_or_try_init(py, || -> PyResult<PyObject> {
            Ok(py
                .import_bound("bytewax.serde")?
                .getattr("PickleSerde")?
                .call0()?
                .unbind())
        })?
        .bind(py))
}
