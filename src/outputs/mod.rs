//! Internal code for output.
//!
//! For a user-centric version of output, read the `bytewax.output`
//! Python module docstring. Read that first.
//!
//! Architecture
//! ------------
//!
//! Output is based around the core trait of [`OutputWriter`].  The
//! [`crate::dataflow::Dataflow::capture`] operator delegates to impls
//! of that trait for actual writing.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`StdOutputConfig`] represents a token in Python for
//! how to create a [`StdOutput`].

use std::collections::HashMap;

use crate::{
    execution::{WorkerCount, WorkerIndex},
    pyo3_extensions::{PyConfigClass, TdPyAny},
};
use pyo3::{exceptions::PyTypeError, prelude::*};

pub(crate) mod kafka_output;
pub(crate) mod manual_epoch_output;
pub(crate) mod manual_output;
pub(crate) mod std_output;

pub(crate) use self::kafka_output::KafkaOutputConfig;
pub(crate) use self::manual_epoch_output::ManualEpochOutputConfig;
pub(crate) use self::manual_output::ManualOutputConfig;
pub(crate) use self::std_output::StdOutputConfig;

/// Base class for an output config.
///
/// These define how a certain stream of data should be output.
///
/// Ues a specific subclass of this that matches the output
/// destination you'd like to write to.
#[pyclass(module = "bytewax.outputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct OutputConfig;

impl OutputConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, OutputConfig {}).unwrap().into()
    }
}

#[pymethods]
impl OutputConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "OutputConfig".into_py(py))]))
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

pub(crate) trait OutputBuilder {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
    ) -> PyResult<Box<dyn OutputWriter<u64, TdPyAny>>>;
}

// Extract a specific OutputConfig from a parent OutputConfig coming from python.
impl PyConfigClass<Box<dyn OutputBuilder>> for Py<OutputConfig> {
    fn downcast(&self, py: Python) -> PyResult<Box<dyn OutputBuilder>> {
        if let Ok(conf) = self.extract::<ManualOutputConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<ManualEpochOutputConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<StdOutputConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<KafkaOutputConfig>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(PyTypeError::new_err(format!(
                "Unknown output_config type: {pytype}"
            )))
        }
    }
}

impl OutputBuilder for Py<OutputConfig> {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
    ) -> PyResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        self.downcast(py)?.build(py, worker_index, worker_count)
    }
}

/// Defines how output of the dataflow is written.
pub(crate) trait OutputWriter<T, D> {
    /// Write a single output item.
    fn push(&mut self, epoch: T, item: D);
}

pub(crate) fn capture(
    writer: &mut Box<dyn OutputWriter<u64, TdPyAny>>,
    epoch: &u64,
    item: &TdPyAny,
) {
    writer.push(*epoch, item.clone());
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<OutputConfig>()?;
    m.add_class::<ManualOutputConfig>()?;
    m.add_class::<ManualEpochOutputConfig>()?;
    m.add_class::<StdOutputConfig>()?;
    m.add_class::<KafkaOutputConfig>()?;
    Ok(())
}
