use pyo3::{exceptions::PyValueError, prelude::*};

use crate::{
    execution::WorkerIndex,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    unwrap_any,
};

use super::{OutputConfig, OutputWriter};

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
pub(crate) struct ManualOutputConfig {
    #[pyo3(get)]
    pub(crate) output_builder: TdPyCallable,
}

#[pymethods]
impl ManualOutputConfig {
    #[new]
    #[args(output_builder)]
    fn new(output_builder: TdPyCallable) -> (Self, OutputConfig) {
        (Self { output_builder }, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualOutputConfig", self.output_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualOutputConfig", output_builder)) = state.extract() {
            self.output_builder = output_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualOutputConfig: {state:?}"
            )))
        }
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
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| unwrap_any!(self.pyfunc.call1(py, (item,))));
    }
}
