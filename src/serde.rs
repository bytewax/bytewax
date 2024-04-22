//! Serialization of Python object for recovery and transport.

use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;

use crate::unwrap_any;

static mut SERDE_OBJ: GILOnceCell<PyObject> = GILOnceCell::new();

/// Load, or return the specified serde object from the Python side
/// into a GILOnceCell.
pub(crate) fn get_serde_obj(py: Python) -> PyResult<&Bound<'_, PyAny>> {
    // SAFETY: Safety here is provided by GilOnceCell requiring the GIL
    // to be held when getting this value.
    unsafe {
        Ok(SERDE_OBJ
            .get(py)
            .expect("No serde object has been set.")
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
fn set_serde_obj(py: Python, serde_object: PyObject) -> PyResult<()> {
    // SAFETY: Safety here is provided by GilOnceCell requiring the GIL
    // to be held when setting this value.
    unsafe {
        // Remove the currently set object.
        SERDE_OBJ.take();
        unwrap_any!(SERDE_OBJ.set(py, serde_object));
    }
    Ok(())
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(set_serde_obj, m)?)?;
    Ok(())
}
