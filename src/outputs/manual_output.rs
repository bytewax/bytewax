use pyo3::prelude::*;

use crate::{
    add_pymethods,
    execution::{WorkerCount, WorkerIndex},
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
        worker_count: WorkerCount,
    ) -> PyResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        Ok(Box::new(ManualOutput::new(
            py,
            self.output_builder.clone(),
            worker_index,
            worker_count,
        )))
    }
}

add_pymethods!(
    ManualOutputConfig,
    parent: OutputConfig,
    py_args: (output_builder),
    args {
        output_builder: TdPyCallable => Python::with_gil(TdPyCallable::pickle_new)
    }
);

/// Call a Python callback function on each item of output.
pub(crate) struct ManualOutput {
    pyfunc: TdPyCallable,
}

impl ManualOutput {
    pub(crate) fn new(
        py: Python,
        output_builder: TdPyCallable,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
    ) -> Self {
        let pyfunc: TdPyCallable = output_builder
            .call1(py, (worker_index.0, worker_count.0))
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
