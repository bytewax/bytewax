//! Serialization of Python object for recovery and transport.

use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;

// This needed to be mutable in order to swap it's value when testing.
static mut SERDE_OBJ: GILOnceCell<PyObject> = GILOnceCell::new();

/// Load, or initialize a Python object to use for Serde
/// into a GILOnceCell.
pub(crate) fn get_serde_obj(py: Python) -> PyResult<&Bound<'_, PyAny>> {
    // SAFETY: Safety here is provided by GilOnceCell requiring the GIL
    // to be held when getting this value.
    unsafe {
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
}

/// Setup Bytewax's internal serde for Python objects
///
/// ```python
/// import json
/// from bytewax.serde import Serde, set_serde_obj
/// from typing import override, Any
///
/// class JSONSerde(Serde):
///     @override
///     def ser(self, obj: Any) -> bytes:
///         return json.dumps(obj).encode("utf-8")
///
///     @override
///     def de(self, s: bytes) -> Any:
///         return json.loads(s)
///
///
/// set_serde_obj(JSONSerde())
///
/// ```
///
/// :arg serde_obj: The instantiated bytewax.serde.Serde class to use
///
/// :type serde_obj: bytewax.serde.Serde
#[pyfunction]
fn set_serde_obj(py: Python, serde_object: &Bound<'_, PyAny>) -> PyResult<()> {
    let ob = serde_object.get_type();
    let superclass = py.import_bound("bytewax.serde")?.getattr("Serde")?;
    if !ob.is_subclass(&superclass)? {
        return Err(PyTypeError::new_err(
            "serialization must subclass `bytewax.serde.Serde`",
        ));
    }
    // SAFETY: Safety here is provided by GilOnceCell requiring the GIL
    // to be held when setting this value.
    let s = serde_object.clone().unbind();
    unsafe {
        // Remove the currently set object.
        SERDE_OBJ.take();
        SERDE_OBJ
            .set(py, s)
            .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?;
    }
    Ok(())
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(set_serde_obj, m)?)?;
    Ok(())
}
