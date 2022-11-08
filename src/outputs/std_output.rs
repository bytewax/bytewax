use pyo3::{ffi::PySys_WriteStdout, prelude::*};
use std::{collections::HashMap, ffi::CString};

use crate::pyo3_extensions::TdPyAny;

use super::{OutputBuilder, OutputConfig, OutputWriter};

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
#[derive(Clone)]
pub(crate) struct StdOutputConfig {}

impl OutputBuilder for StdOutputConfig {
    fn build(
        &self,
        py: Python,
        _worker_index: crate::execution::WorkerIndex,
        _worker_count: usize,
    ) -> crate::common::StringResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        Ok(Box::new(py.allow_threads(StdOutput::new)))
    }
}

#[pymethods]
impl StdOutputConfig {
    #[new]
    #[args()]
    fn new() -> (Self, OutputConfig) {
        (Self {}, OutputConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "StdOutputConfig".into_py(py))]))
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
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
    #[tracing::instrument(name = "StdOutput.push", level = "trace", skip_all)]
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
