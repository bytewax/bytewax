//!

use crate::pyo3_extensions::{TdPyAny, TdPyIterator};
use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use send_wrapper::SendWrapper;
use std::time::{Duration, Instant};
use timely::dataflow::InputHandle;

/// Advance to the supplied epoch.
///
/// When providing input to a Dataflow, work cannot complete until
/// there is no more data for a given epoch.
///
/// AdvanceTo is the signal to a Dataflow that the frontier has moved
/// beyond the current epoch, and that items with an epoch less than
/// the epoch in AdvanceTo can be worked to completion.
///
/// Using AdvanceTo and Emit is only necessary when using `spawn_cluster`
/// and `cluster_main()` as `run()` and `run_cluster()` will yield AdvanceTo
/// and Emit for you. Likewise, they are only required when using a
/// manual input configuration.
///
/// See also: `inputs.yield_epochs()`
///
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
#[pyclass(module = "bytewax.inputs")]
#[pyo3(text_signature = "(epoch)")]
pub(crate) struct AdvanceTo {
    #[pyo3(get)]
    epoch: u64,
}

#[pymethods]
impl AdvanceTo {
    #[new]
    fn new(epoch: u64) -> Self {
        Self { epoch }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self.epoch < other.epoch),
            CompareOp::Le => Ok(self.epoch <= other.epoch),
            CompareOp::Eq => Ok(self.epoch == other.epoch),
            CompareOp::Ne => Ok(self.epoch != other.epoch),
            CompareOp::Gt => Ok(self.epoch > other.epoch),
            CompareOp::Ge => Ok(self.epoch >= other.epoch),
        }
    }
}

/// Emit the supplied item into the dataflow at the current epoch
///
/// Emit is how we introduce input into a dataflow using a manual
/// input configuration:
///
/// >>> def input_builder(worker_index, worker_count, resume_epoch):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
#[pyclass(module = "bytewax.inputs")]
#[pyo3(text_signature = "(item)")]
pub(crate) struct Emit {
    #[pyo3(get)]
    item: TdPyAny,
}

#[pymethods]
impl Emit {
    #[new]
    fn new(item: Py<PyAny>) -> Self {
        Self { item: item.into() }
    }
}

/// Base class for an input config.
///
/// InputConfig defines how you will input data to your dataflow.
///
/// Use a specific subclass of InputConfig for the kind of input
/// source you are plan to use. See the subclasses in this module.
#[pyclass(module = "bytewax.inputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct InputConfig;

/// Use [Kafka](https://kafka.apache.org) as the input source. Currently
/// does not support recovery. Kafka messages will be passed through the dataflow
/// as byte two-tuples of Kafka key and payload.
///
/// Currently Kafka input does not support recovery.
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
///     epoch_length: (timedelta)
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to
///     your execution entry point.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(
    text_signature = "(brokers, group_id, topics, tail, offset_reset, auto_commit, epoch_length)"
)]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    pub brokers: Vec<String>,
    #[pyo3(get)]
    pub group_id: String,
    #[pyo3(get)]
    pub topics: Vec<String>,
    #[pyo3(get)]
    pub tail: bool,
    #[pyo3(get)]
    pub offset_reset: String,
    #[pyo3(get)]
    pub auto_commit: bool,
    #[pyo3(get)]
    epoch_length: pyo3_chrono::Duration,
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
        auto_commit = false,
        epoch_length = "pyo3_chrono::Duration(chrono::Duration::seconds(5))"
    )]
    fn new(
        brokers: Vec<String>,
        group_id: String,
        topics: Vec<String>,
        tail: bool,
        offset_reset: String,
        auto_commit: bool,
        epoch_length: pyo3_chrono::Duration,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                group_id,
                topics,
                tail,
                offset_reset,
                auto_commit,
                epoch_length,
            },
            InputConfig {},
        )
    }

    fn __getstate__(
        &self,
    ) -> (
        &str,
        Vec<String>,
        String,
        Vec<String>,
        bool,
        String,
        bool,
        pyo3_chrono::Duration,
    ) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.group_id.clone(),
            self.topics.clone(),
            self.tail,
            self.offset_reset.clone(),
            self.auto_commit,
            self.epoch_length,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(
        &self,
    ) -> (
        Vec<String>,
        &str,
        Vec<String>,
        bool,
        &str,
        bool,
        pyo3_chrono::Duration,
    ) {
        let s = "UNINIT_PICKLED_STRING";
        (
            Vec::new(),
            s,
            Vec::new(),
            false,
            s,
            false,
            pyo3_chrono::Duration(chrono::Duration::zero()),
        )
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
            epoch_length,
        )) = state.extract()
        {
            self.brokers = brokers;
            self.group_id = group_id;
            self.topics = topics;
            self.tail = tail;
            self.offset_reset = offset_reset;
            self.auto_commit = auto_commit;
            self.epoch_length = epoch_length;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaInputConfig: {state:?}"
            )))
        }
    }
}

/// Use a user-defined function that returns an iterable as the input source.
///
/// It is your responsibility to design your input handlers in such a
/// way that it jumps to the point in the input that corresponds to
/// the `resume_epoch` argument; if it can't (because the input is
/// ephemeral) you can still recover the dataflow, but the lost input
/// is unable to be replayed so the output will be different.
///
/// Args:
///
///     input_builder: An input_builder function that yields `AdvanceTo()` or `Emit()`
///         with this worker's input. Must resume from the epoch specified.
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to
///     your execution entry point.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(input_builder)")]
pub(crate) struct ManualInputConfig {
    input_builder: Py<PyAny>,
}

#[pymethods]
impl ManualInputConfig {
    #[new]
    #[args(input_builder)]
    fn new(input_builder: Py<PyAny>) -> (Self, InputConfig) {
        (Self { input_builder }, InputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self, py: Python) -> (&str, Py<PyAny>) {
        ("ManualInputConfig", self.input_builder.clone_ref(py))
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str,) {
        (&"",)
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

/// Take a single data element or timestamp and feed it into dataflow
/// Pump will be called in the worker's "main" loop to feed data in.
pub trait Pump {
    fn pump(&mut self);
    fn input_time(&mut self) -> &u64;
    fn input_remains(&mut self) -> bool;
}

/// Encapsulates the process of pulling data out of a Kafka
/// stream and feeding it into Timely.
struct KafkaPump {
    consumer: BaseConsumer,
    push_to_timely: InputHandle<u64, TdPyAny>,
    epoch_length: Duration,
    epoch_start: Instant,
    empty: bool,
}

impl KafkaPump {
    // TODO: Support recovery epoch on Kafka input. That'll require
    // storing epoch to offset mappings.
    fn new(
        brokers: &[String],
        group_id: &str,
        topics: &[String],
        tail: bool,
        offset_reset: &str,
        auto_commit: bool,
        epoch_length: Duration,
        push_to_timely: InputHandle<u64, TdPyAny>,
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

        Self {
            consumer,
            push_to_timely,
            epoch_length,
            epoch_start: Instant::now(),
            empty: false,
        }
    }
}

impl Pump for KafkaPump {
    fn pump(&mut self) {
        let now = Instant::now();
        if now > self.epoch_start + self.epoch_length {
            self.push_to_timely
                .advance_to(self.push_to_timely.time() + 1);
            self.epoch_start = now;
        }

        // TODO: Figure out how we're going to rate limit all
        // inputs. I think we could do something like "activate_delay"
        // on the input source itself once we move from Pumps to
        // sources.
        match self.consumer.poll(Duration::from_millis(100)) {
            Some(Err(KafkaError::PartitionEOF(_))) => {
                self.empty = true;
            }
            Some(Err(err)) => panic!("Error reading input from Kafka topic: {err:?}"),
            None => {}
            Some(Ok(s)) => Python::with_gil(|py| {
                let key: Py<PyAny> = s.key().map_or(py.None(), |k| PyBytes::new(py, k).into());
                let payload: Py<PyAny> = s
                    .payload()
                    .map_or(py.None(), |k| PyBytes::new(py, k).into());
                let key_payload: Py<PyAny> = (key, payload).into_py(py);

                self.push_to_timely.send(key_payload.into())
            }),
        }
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.empty
    }
}

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
struct ManualPump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl ManualPump {
    fn new(
        py: Python,
        worker_index: usize,
        worker_count: usize,
        resume_epoch: u64,
        config: PyRef<ManualInputConfig>,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        let worker_input: TdPyIterator = config
            .input_builder
            .call1(py, (worker_index, worker_count, resume_epoch))
            .unwrap()
            .extract(py)
            .unwrap();
        Self {
            pull_from_pyiter: worker_input,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }
}

impl Pump for ManualPump {
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(input_or_action) = pull_from_pyiter.next() {
                match input_or_action {
                    Ok(item) => {
                        if let Ok(send) = item.downcast::<PyCell<Emit>>() {
                            self.push_to_timely.send(send.borrow().item.clone());
                        } else if let Ok(advance_to) = item.downcast::<PyCell<AdvanceTo>>() {
                            let epoch = advance_to.borrow().epoch;
                            let input_epoch = self.push_to_timely.time();
                            if &epoch >= input_epoch {
                                self.push_to_timely.advance_to(epoch)
                            } else {
                                panic!("The epoch on `AdvanceTo` must never decrease; got an epoch of `{epoch:?}` while current input epoch is `{input_epoch:?}`; are you respecting `resume_epoch`?");
                            }
                        } else {
                            panic!("Input must be an instance of either `AdvanceTo` or `Emit`; got `{item:?}` instead. See https://docs.bytewax.io/apidocs#bytewax.AdvanceTo for more information.");
                        }
                    }
                    Err(err) => {
                        std::panic::panic_any(err);
                    }
                }
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.pyiter_is_empty
    }
}

pub(crate) fn pump_from_config(
    py: Python,
    input_config: Py<InputConfig>,
    input_handle: InputHandle<u64, TdPyAny>,
    worker_index: usize,
    worker_count: usize,
    resume_epoch: u64,
) -> Box<dyn Pump> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let input_config = input_config.as_ref(py);

    if let Ok(kafka_config) = input_config.downcast::<PyCell<KafkaInputConfig>>() {
        let kafka_config = kafka_config.borrow();

        let brokers = &kafka_config.brokers;
        let group_id = &kafka_config.group_id;
        let topics = &kafka_config.topics;
        let tail = kafka_config.tail;
        let offset_reset = &kafka_config.offset_reset;
        let auto_commit = kafka_config.auto_commit;
        let epoch_length = kafka_config
            .epoch_length
            .0
            .to_std()
            .expect("Can't convert epoch_length into std::time::Duration; negative length?");

        let input_handle = SendWrapper::new(input_handle);
        let kafka_pump = py.allow_threads(|| {
            SendWrapper::new(KafkaPump::new(
                brokers,
                group_id,
                topics,
                tail,
                offset_reset,
                auto_commit,
                epoch_length,
                input_handle.take(),
            ))
        });

        Box::new(kafka_pump.take())
    } else if let Ok(manual_config) = input_config.downcast::<PyCell<ManualInputConfig>>() {
        let manual_config = manual_config.borrow();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let manual_pump = ManualPump::new(
            py,
            worker_index,
            worker_count,
            resume_epoch,
            manual_config,
            input_handle,
        );

        Box::new(manual_pump)
    } else {
        let pytype = input_config.get_type();
        panic!("Unknown input_config type: {pytype}")
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Emit>()?;
    m.add_class::<AdvanceTo>()?;
    m.add_class::<InputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    Ok(())
}
