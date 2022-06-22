use crate::pyo3_extensions::{TdPyAny, TdPyIterator};

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::message::Message;
use std::time::Duration;
use timely::dataflow::InputHandle;
use tokio::runtime::Runtime;

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
///
/// Args:
///
///     brokers: Comma-separated of broker addresses.
///         E.g. "localhost:9092,localhost:9093"
///
///     group_id: Group id as a string.
///
///     topic: Topic to which consumer will subscribe.
///
///    offset_reset: Can be "earliest" or "latest". Delegates where to resume if
///         auto_commit is not enabled. Defaults to "earliest".
///
///     auto_commit: If true, commit offset of the last message handed to the
///         application. This committed offset will be used when the process
///         restarts to pick up where it left off. Defaults to false.
///
///     messages_per_epoch: (integer) Defines maximum number of messages per epoch.
///         Defaults to `1`. If the consumer times out waiting, the system will
///         increment to the next epoch, and fewer (or no) messages may be assigned
///         to the preceding epoch.
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to
///     your execution entry point.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(
    text_signature = "(brokers, group_id, topics, offset_reset, auto_commit, messages_per_epoch)"
)]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    pub brokers: String,
    #[pyo3(get)]
    pub group_id: String,
    #[pyo3(get)]
    pub topics: String,
    #[pyo3(get)]
    pub offset_reset: String,
    #[pyo3(get)]
    pub auto_commit: bool,
    #[pyo3(get)]
    pub messages_per_epoch: u64,
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(
        brokers,
        group_id,
        topics,
        offset_reset = "\"earliest\".to_string()",
        auto_commit = false,
        messages_per_epoch = 1
    )]
    fn new(
        brokers: String,
        group_id: String,
        topics: String,
        offset_reset: String,
        auto_commit: bool,
        messages_per_epoch: u64,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                group_id,
                topics,
                offset_reset,
                auto_commit,
                messages_per_epoch,
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, String, String, String, String, u64) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.group_id.clone(),
            self.topics.clone(),
            self.offset_reset.clone(),
            self.messages_per_epoch,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str, &str, &str, &str, u64) {
        let s = "UNINIT_PICKLED_STRING";
        (s, s, s, s, 0)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok((
            "KafkaInputConfig",
            brokers,
            group_id,
            topics,
            offset_reset,
            messages_per_epoch,
        )) = state.extract()
        {
            self.brokers = brokers;
            self.group_id = group_id;
            self.topics = topics;
            self.offset_reset = offset_reset;
            self.messages_per_epoch = messages_per_epoch;
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
    consumer: BaseConsumer<CustomContext>,
    rt: Runtime,
    push_to_timely: InputHandle<u64, TdPyAny>,
    desired_messages_per_epoch: u64,
    current_messages_per_epoch: u64,
    current_epoch: u64,
    empty: bool,
}

impl KafkaPump {
    fn new(config: PyRef<KafkaInputConfig>, push_to_timely: InputHandle<u64, TdPyAny>) -> Self {
        let context = CustomContext;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let consumer: BaseConsumer<CustomContext> = rt.block_on(async {
            ClientConfig::new()
                .set("group.id", config.group_id.clone())
                .set("bootstrap.servers", config.brokers.clone())
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", config.auto_commit.to_string())
                .set("auto.offset.reset", config.offset_reset.clone())
                .set_log_level(RDKafkaLogLevel::Debug)
                .create_with_context(context)
                .expect("Consumer creation failed")
        });

        consumer
            .subscribe(&[&config.topics])
            .expect("Can't subscribe to specified topics");

        Self {
            consumer,
            rt,
            push_to_timely,
            current_epoch: 0,
            desired_messages_per_epoch: config.messages_per_epoch,
            current_messages_per_epoch: 0,
            empty: false,
        }
    }
}

impl Pump for KafkaPump {
    fn pump(&mut self) {
        if self.current_messages_per_epoch == self.desired_messages_per_epoch {
            self.current_messages_per_epoch = 0;
            self.current_epoch += 1;
            self.push_to_timely.advance_to(self.current_epoch);
        } else {
            self.rt.block_on(async {
                match self.consumer.poll(Duration::from_millis(1000)) {
                    None => {
                        dbg!("No messages available, incrementing epoch");
                        self.current_messages_per_epoch = 0;
                        self.current_epoch += 1;
                        self.push_to_timely.advance_to(self.current_epoch);
                    }
                    Some(r) => match r {
                        Err(e) => panic!("Kafka error! {}", e),
                        Ok(s) => {
                            self.current_messages_per_epoch += 1;
                            Python::with_gil(|py| {
                                let key: Py<PyAny> =
                                    s.key().map_or(py.None(), |k| PyBytes::new(py, k).into());
                                let payload: Py<PyAny> = s
                                    .payload()
                                    .map_or(py.None(), |k| PyBytes::new(py, k).into());
                                let key_payload: Py<PyAny> = (key, payload).into_py(py);

                                self.push_to_timely.send(key_payload.into())
                            })
                        }
                    },
                }
            })
        }
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    // Always true right now
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
                            self.push_to_timely.advance_to(advance_to.borrow().epoch);
                        } else {
                            panic!("{}", format!("Input must be an instance of either `AdvanceTo` or `Emit`. Got: {item:?}. See https://docs.bytewax.io/apidocs#bytewax.AdvanceTo for more information."))
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
    config: Py<InputConfig>,
    input_handle: InputHandle<u64, TdPyAny>,
    worker_index: usize,
    worker_count: usize,
    resume_epoch: u64,
) -> Box<dyn Pump> {
    let input_config = config.as_ref(py);
    if let Ok(kafka_config) = input_config.downcast::<PyCell<KafkaInputConfig>>() {
        let kafka_config = kafka_config.borrow();
        Box::new(KafkaPump::new(kafka_config, input_handle))
    } else if let Ok(manual_config) = input_config.downcast::<PyCell<ManualInputConfig>>() {
        let manual_config = manual_config.borrow();
        Box::new(ManualPump::new(
            py,
            worker_index,
            worker_count,
            resume_epoch,
            manual_config,
            input_handle,
        ))
    } else {
        let pytype = input_config.get_type();
        panic!("Unknown input_config type: {pytype}")
    }
}

//  // A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// We're not using this yet, but seems like it will be helpful with recovery callbacks
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Emit>()?;
    m.add_class::<AdvanceTo>()?;
    m.add_class::<InputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    Ok(())
}
