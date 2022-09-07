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

use crate::{pyo3_extensions::TdPyAny, StringResult};
use pyo3::{exceptions::PyValueError, prelude::*};
use send_wrapper::SendWrapper;

pub(crate) mod kafka_output;
pub(crate) mod manual_epoch_output;
pub(crate) mod manual_output;
pub(crate) mod std_output;

pub(crate) use self::kafka_output::{KafkaOutput, KafkaOutputConfig};
pub(crate) use self::manual_epoch_output::{ManualEpochOutput, ManualEpochOutputConfig};
pub(crate) use self::manual_output::{ManualOutput, ManualOutputConfig};
pub(crate) use self::std_output::{StdOutput, StdOutputConfig};

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

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("OutputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("OutputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for OutputConfig: {state:?}"
            )))
        }
    }
}

/// Defines how output of the dataflow is written.
pub(crate) trait OutputWriter<T, D> {
    /// Write a single output item.
    fn push(&mut self, epoch: T, item: D);
}

pub(crate) fn build_output_writer(
    py: Python,
    config: Py<OutputConfig>,
    worker_index: usize,
    worker_count: usize,
) -> StringResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let config = config.as_ref(py);

    if let Ok(config) = config.downcast::<PyCell<ManualOutputConfig>>() {
        let config = config.borrow();

        let output_builder = config.output_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let writer = ManualOutput::new(py, output_builder, worker_index, worker_count);

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<ManualEpochOutputConfig>>() {
        let config = config.borrow();

        let output_builder = config.output_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let writer = ManualEpochOutput::new(py, output_builder, worker_index, worker_count);

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<StdOutputConfig>>() {
        let _config = config.borrow();

        let writer = py.allow_threads(StdOutput::new);

        Ok(Box::new(writer))
    } else if let Ok(config) = config.downcast::<PyCell<KafkaOutputConfig>>() {
        let config = config.borrow();

        let brokers = &config.brokers;
        let topic = &config.topic;
        let additional_properties = &config.additional_properties;

        let writer = py.allow_threads(|| {
            SendWrapper::new(KafkaOutput::new(
                brokers,
                topic.to_string(),
                additional_properties,
            ))
        });

        Ok(Box::new(writer.take()))
    } else {
        let pytype = config.get_type();
        Err(format!("Unknown output_config type: {pytype}"))
    }
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
