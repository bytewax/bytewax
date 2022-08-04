//! Internal code for input systems.
//!
//! For a user-centric version of input, read the `bytewax.input`
//! Python module docstring. Read that first.

use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use send_wrapper::SendWrapper;
use std::task::Poll;
use std::time::Duration;

/// Base class for an input config.
///
/// These define how you will input data to your dataflow.
///
/// Use a specific subclass of InputConfig for the kind of input
/// source you are plan to use. See the subclasses in this module.
#[pyclass(module = "bytewax.inputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct InputConfig;

impl InputConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, InputConfig {}).unwrap().into()
    }
}

#[pymethods]
impl InputConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("InputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("InputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for InputConfig: {state:?}"
            )))
        }
    }
}

/// Use a user-defined function that returns an iterable as the input
/// source.
///
/// Currently does not support recovery.
///
/// Args:
///
///     input_builder: `input_builder(worker_index: int, worker_count:
///         int) => Iterator[Any]` Builder function which returns an
///         iterator of the input that worker should introduce into
///         the dataflow. Note that e.g. returning the same list from
///         each worker will result in duplicate data in the dataflow.
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to the
///     `bytewax.dataflow.Dataflow` constructor.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(input_builder)")]
pub(crate) struct ManualInputConfig {
    #[pyo3(get)]
    input_builder: TdPyCallable,
}

#[pymethods]
impl ManualInputConfig {
    #[new]
    #[args(input_builder)]
    fn new(input_builder: TdPyCallable) -> (Self, InputConfig) {
        (Self { input_builder }, InputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualInputConfig", self.input_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualInputConfig", input_builder)) = state.extract() {
            self.input_builder = input_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualInputConfig: {state:?}"
            )))
        }
    }
}

/// Use [Kafka](https://kafka.apache.org) as the input
/// source.
///
/// Kafka messages will be passed through the dataflow as two-tuples
/// of `(key_bytes, payload_bytes)`.
///
/// Currently does not support recovery.
///
/// Args:
///
///     brokers: Broker addresses. E.g. "localhost:9092,localhost:9093"
///
///     group_id: Group id as a string.
///
///     topics: Topics to which consumer will subscribe.
///
///     offset_reset: Can be "earliest" or "latest". Delegates where
///         to resume if auto_commit is not enabled. Defaults to
///         "earliest".
///
///     auto_commit: If true, commit offset of the last message handed
///         to the application. This committed offset will be used
///         when the process restarts to pick up where it left
///         off. Defaults to false.
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to the
///     `bytewax.dataflow.Dataflow` constructor.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(brokers, group_id, topics, tail, offset_reset, auto_commit)")]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    brokers: Vec<String>,
    #[pyo3(get)]
    group_id: String,
    #[pyo3(get)]
    topics: Vec<String>,
    #[pyo3(get)]
    tail: bool,
    #[pyo3(get)]
    offset_reset: String,
    #[pyo3(get)]
    auto_commit: bool,
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(
        brokers,
        group_id,
        topics,
        tail = true,
        offset_reset = "\"earliest\".to_string()",
        auto_commit = false
    )]
    fn new(
        brokers: Vec<String>,
        group_id: String,
        topics: Vec<String>,
        tail: bool,
        offset_reset: String,
        auto_commit: bool,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                group_id,
                topics,
                tail,
                offset_reset,
                auto_commit,
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String, Vec<String>, bool, String, bool) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.group_id.clone(),
            self.topics.clone(),
            self.tail,
            self.offset_reset.clone(),
            self.auto_commit,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, Vec<String>, bool, &str, bool) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, Vec::new(), false, s, false)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok((
            "KafkaInputConfig",
            brokers,
            group_id,
            topics,
            tail,
            offset_reset,
            auto_commit,
        )) = state.extract()
        {
            self.brokers = brokers;
            self.group_id = group_id;
            self.topics = topics;
            self.tail = tail;
            self.offset_reset = offset_reset;
            self.auto_commit = auto_commit;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaInputConfig: {state:?}"
            )))
        }
    }
}

/// Defines how a single source of input reads data.
pub(crate) trait InputReader<D> {
    /// This has the same semantics as
    /// [`std::async_iter::AsyncIterator::poll_next`]:
    ///
    /// - [`Poll::Pending`]: no new values ready yet.
    ///
    /// - [`Poll::Ready`] with a [`Some`]: a new value has arrived.
    ///
    /// - [`Poll::Ready`] with a [`None`]: the stream has ended and
    ///   [`next`] should not be called again.
    fn next(&mut self) -> Poll<Option<D>>;
}

pub(crate) fn build_input_reader(
    py: Python,
    config: Py<InputConfig>,
    worker_index: usize,
    worker_count: usize,
) -> Result<Box<dyn InputReader<TdPyAny>>, String> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let config = config.as_ref(py);

    if let Ok(config) = config.downcast::<PyCell<ManualInputConfig>>() {
        let config = config.borrow();

        let input_builder = config.input_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let reader = ManualInput::new(py, input_builder, worker_index, worker_count);

        Ok(Box::new(reader))
    } else if let Ok(config) = config.downcast::<PyCell<KafkaInputConfig>>() {
        let config = config.borrow();

        let brokers = &config.brokers;
        let group_id = &config.group_id;
        let topics = &config.topics;
        let tail = config.tail;
        let offset_reset = &config.offset_reset;
        let auto_commit = config.auto_commit;

        let reader = py.allow_threads(|| {
            SendWrapper::new(KafkaInput::new(
                brokers,
                group_id,
                topics,
                tail,
                offset_reset,
                auto_commit,
            ))
        });

        Ok(Box::new(reader.take()))
    } else {
        let pytype = config.get_type();
        Err(format!("Unknown input_config type: {pytype}"))
    }
}

/// Construct a Python iterator for each worker from a builder
/// function.
struct ManualInput {
    pyiter: TdPyIterator,
}

impl ManualInput {
    fn new(
        py: Python,
        input_builder: TdPyCallable,
        worker_index: usize,
        worker_count: usize,
    ) -> Self {
        let pyiter: TdPyIterator = input_builder
            .call1(py, (worker_index, worker_count))
            .unwrap()
            .extract(py)
            .unwrap();
        Self { pyiter }
    }
}

impl InputReader<TdPyAny> for ManualInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        Poll::Ready(self.pyiter.next())
    }
}

/// Read from Kafka for an input source.
struct KafkaInput {
    consumer: BaseConsumer,
}

impl KafkaInput {
    fn new(
        brokers: &[String],
        group_id: &str,
        topics: &[String],
        tail: bool,
        offset_reset: &str,
        auto_commit: bool,
    ) -> Self {
        let eof = !tail;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers.join(","))
            .set("enable.partition.eof", format!("{eof}"))
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", format!("{auto_commit}"))
            .set("auto.offset.reset", offset_reset)
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .expect("Consumer creation failed");

        let topics: Vec<_> = topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .expect("Can't subscribe to specified topics");

        Self { consumer }
    }
}

impl InputReader<TdPyAny> for KafkaInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        match self.consumer.poll(Duration::from_millis(0)) {
            None => Poll::Pending,
            Some(Err(KafkaError::PartitionEOF(_))) => Poll::Ready(None),
            Some(Err(err)) => panic!("Error reading input from Kafka topic: {err:?}"),
            Some(Ok(s)) => Python::with_gil(|py| {
                let key: Py<PyAny> = s.key().map_or(py.None(), |k| PyBytes::new(py, k).into());
                let payload: Py<PyAny> = s
                    .payload()
                    .map_or(py.None(), |k| PyBytes::new(py, k).into());
                let key_payload: Py<PyAny> = (key, payload).into_py(py);

                Poll::Ready(Some(key_payload.into()))
            }),
        }
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<InputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    Ok(())
}
