use std::collections::HashMap;

use pyo3::{prelude::*, types::PyDict};

use crate::{
    common::pickle_extract,
    execution::WorkerIndex,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    unwrap_any,
};

use super::{OutputBuilder, OutputConfig, OutputWriter};

/// Call a Python callback function with each output item.
///
/// Args:
///
///   output_builder: `output_builder(worker_index: int,
///       worker_count: int) => output_handler(item: Any)` Builder
///       function which returns a handler function for each worker
///       thread, called with `item` whenever an item passes by this
///       capture operator on this worker.
///
/// Returns:
///
///   Config object. Pass this to the
///   `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig, subclass)]
#[pyo3(text_signature = "(output_builder)")]
#[derive(Clone)]
pub(crate) struct ManualOutputConfig {
    #[pyo3(get)]
    pub(crate) output_builder: TdPyCallable,
}

impl OutputBuilder for ManualOutputConfig {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: usize,
    ) -> crate::common::StringResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        Ok(Box::new(ManualOutput::new(
            py,
            self.output_builder.clone(),
            worker_index,
            worker_count,
        )))
    }
}

#[pymethods]
impl ManualOutputConfig {
    #[new]
    #[args(output_builder)]
    fn new(output_builder: TdPyCallable) -> (Self, OutputConfig) {
        (Self { output_builder }, OutputConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "ManualOutputConfig".into_py(py)),
                ("output_builder", self.output_builder.clone().into_py(py)),
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from a PyDict
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.output_builder = pickle_extract(dict, "output_builder")?;
        Ok(())
    }
}

/// Call a Python callback function on each item of output.
pub(crate) struct ManualOutput {
    pyfunc: TdPyCallable,
}

impl ManualOutput {
    pub(crate) fn new(
        py: Python,
        output_builder: TdPyCallable,
        worker_index: WorkerIndex,
        worker_count: usize,
    ) -> Self {
        let pyfunc: TdPyCallable = output_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        Self { pyfunc }
    }
}

impl OutputWriter<u64, TdPyAny> for ManualOutput {
    #[tracing::instrument(name = "ManualOutput.push", level = "trace", skip_all)]
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| unwrap_any!(self.pyfunc.call1(py, (item,))));
    }
}
