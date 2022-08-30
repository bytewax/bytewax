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

use std::ffi::CString;

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    unwrap_any, StringResult,
};
use pyo3::{exceptions::PyValueError, prelude::*};
use pyo3::ffi::PySys_WriteStdout;
use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use send_wrapper::SendWrapper;
use std::time::Duration;

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
    output_builder: TdPyCallable,
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
    output_builder: TdPyCallable,
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

/// Use [Kafka](https://kafka.apache.org) as the output.
///
/// A `capture` using KafkaOutput expects to receive data
/// structured as two-tuples of (key, payload) to form a Kafka
/// record.
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic (str): Topic to which producer will send records.
///
/// Returns:
///
///   Config object. Pass this as the `output_config` argument to the
///   `bytewax.dataflow.Dataflow.output`.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(brokers, topic)")]
pub(crate) struct KafkaOutputConfig {
    #[pyo3(get)]
    brokers: Vec<String>,
    #[pyo3(get)]
    topic: String,
}

#[pymethods]
impl KafkaOutputConfig {
    #[new]
    #[args(brokers, topic)]
    fn new(brokers: Vec<String>, topic: String) -> (Self, OutputConfig) {
        (Self { brokers, topic }, OutputConfig {})
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String) {
        (
            "KafkaOutputConfig",
            self.brokers.clone(),
            self.topic.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaOutputConfig", brokers, topic)) = state.extract() {
            self.brokers = brokers;
            self.topic = topic;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaOutputConfig: {state:?}"
            )))
        }
    }
}

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

        let writer =
            py.allow_threads(|| SendWrapper::new(KafkaOutput::new(brokers, topic.to_string())));

        Ok(Box::new(writer.take()))
    } else {
        let pytype = config.get_type();
        Err(format!("Unknown output_config type: {pytype}"))
    }
}

/// Call a Python callback function on each item of output.
struct ManualOutput {
    pyfunc: TdPyCallable,
}

impl ManualOutput {
    fn new(
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

impl OutputWriter<u64, TdPyAny> for ManualOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        Python::with_gil(|py| unwrap_any!(self.pyfunc.call1(py, (item,))));
    }
}

/// Call a Python callback function on each item of output with its
/// epoch.
struct ManualEpochOutput {
    pyfunc: TdPyCallable,
}

impl ManualEpochOutput {
    fn new(
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

/// Print output to standard out.
struct StdOutput {}

impl StdOutput {
    fn new() -> Self {
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

/// Produce output to kafka stream
struct KafkaOutput {
    producer: BaseProducer,
    topic: String,
}

impl KafkaOutput {
    fn new(brokers: &[String], topic: String) -> Self {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .expect("Producer creation error");

        Self { producer, topic }
    }
}

impl OutputWriter<u64, TdPyAny> for KafkaOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        let is = item.to_string();
        self.producer
            .send(
                BaseRecord::to(&self.topic)
                    .payload(&is)
                    // errr what's the key
                    .key("and this is a key"),
            )
            .expect("Failed to enqueue");

        // Poll to process all the asynchronous delivery events.
        self.producer.poll(Duration::from_millis(0));

        // And/or flush the producer before dropping it.
        self.producer.flush(Duration::from_secs(1));
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
