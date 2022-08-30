use pyo3::{exceptions::PyValueError, ffi::PySys_WriteStdout, prelude::*};
use std::ffi::CString;

use crate::pyo3_extensions::TdPyAny;

use super::{OutputConfig, OutputWriter};

/// Write the output items to standard out.
///
/// Items must have a valid `__str__`. If not, map the items into a
/// string before capture.
///
/// Returns:
///
///   Config object. Pass this to the
///   `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "()")]
pub(crate) struct StdOutputConfig {}

#[pymethods]
impl StdOutputConfig {
    #[new]
    #[args()]
    fn new() -> (Self, OutputConfig) {
        (Self {}, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("StdOutputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("StdOutputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for StdOutputConfig: {state:?}"
            )))
        }
    }
}

/// Print output to standard out.
pub(crate) struct StdOutput {}

impl StdOutput {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl OutputWriter<u64, TdPyAny> for StdOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| {
            let item = item.as_ref(py);
            let item_str: &str = item
                .str()
                .expect("Items written to std out need to implement `__str__`")
                .extract()
                .unwrap();
            let output = CString::new(format!("{item_str}\n")).unwrap();
            let stdout_str = output.as_ptr() as *const i8;
            unsafe {
                PySys_WriteStdout(stdout_str);
            }
        });
    }
}
