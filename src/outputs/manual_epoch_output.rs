use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    unwrap_any,
};
use pyo3::{exceptions::PyValueError, prelude::*};

use super::{OutputConfig, OutputWriter};

/// Call a Python callback function with each output epoch and item.
///
/// You probably want to use `ManualOutputConfig` unless you know you
/// need specific epoch assignments for deep integration work.
///
/// Args:
///
///   output_builder: `output_builder(worker_index: int,
///       worker_count: int) => output_handler(epoch_item:
///       Tuple[int, Any])` Builder function which returns a handler
///       function for each worker thread, called with `(epoch,
///       item)` whenever an item passes by this capture operator on
///       this worker.
///
/// Returns:
///
///   Config object. Pass this to the
///   `bytewax.dataflow.Dataflow.capture` operator.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig, subclass)]
#[pyo3(text_signature = "(output_builder)")]
pub(crate) struct ManualEpochOutputConfig {
    pub(crate) output_builder: TdPyCallable,
}

#[pymethods]
impl ManualEpochOutputConfig {
    #[new]
    #[args(output_builder)]
    fn new(output_builder: TdPyCallable) -> (Self, OutputConfig) {
        (Self { output_builder }, OutputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualEpochOutputConfig", self.output_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualEpochOutputConfig", output_builder)) = state.extract() {
            self.output_builder = output_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualEpochOutputConfig: {state:?}"
            )))
        }
    }
}

/// Call a Python callback function on each item of output with its
/// epoch.
pub(crate) struct ManualEpochOutput {
    pyfunc: TdPyCallable,
}

impl ManualEpochOutput {
    pub(crate) fn new(
        py: Python,
        output_builder: TdPyCallable,
        worker_index: usize,
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

impl OutputWriter<u64, TdPyAny> for ManualEpochOutput {
    fn push(&mut self, epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| {
            let epoch_item_pytuple: Py<PyAny> = (epoch, item).into_py(py);
            unwrap_any!(self.pyfunc.call1(py, (epoch_item_pytuple,)))
        });
    }
}
