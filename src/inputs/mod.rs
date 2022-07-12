use crate::pyo3_extensions::TdPyCallable;
use crate::pyo3_extensions::{TdPyAny, TdPyIterator};

use chrono::Utc;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_chrono::Duration as PyDuration;
use pyo3_chrono::NaiveDateTime as PyNaiveDateTime;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::message::{BorrowedMessage, Message};
use std::time::Duration as StdDuration;
use timely::dataflow::InputHandle;
use tokio::runtime::Runtime;

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
#[pyo3(text_signature = "(brokers, group_id, topics, offset_reset, auto_commit)")]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    pub brokers: String,
    #[pyo3(get)]
    pub group_id: String,
    #[pyo3(get)]
    pub topics: String,
    #[pyo3(get)]
    pub deserializer: Option<TdPyCallable>,
    #[pyo3(get)]
    pub offset_reset: String,
    #[pyo3(get)]
    pub auto_commit: bool,
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
        deserializer = "None"
    )]
    fn new(
        brokers: String,
        group_id: String,
        topics: String,
        offset_reset: String,
        auto_commit: bool,
        deserializer: Option<TdPyCallable>,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                group_id,
                topics,
                offset_reset,
                auto_commit,
                deserializer,
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, String, String, String, String, Option<TdPyCallable>) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.group_id.clone(),
            self.topics.clone(),
            self.offset_reset.clone(),
            self.deserializer.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str, &str, &str, &str, Option<TdPyCallable>) {
        let s = "UNINIT_PICKLED_STRING";
        (s, s, s, s, None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaInputConfig", brokers, group_id, topics, offset_reset, deserializer)) =
            state.extract()
        {
            self.brokers = brokers;
            self.group_id = group_id;
            self.topics = topics;
            self.offset_reset = offset_reset;
            self.deserializer = deserializer;
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
#[pyo3(text_signature = "(input_builder, deserializer)")]
pub(crate) struct ManualInputConfig {
    input_builder: Py<PyAny>,
    deserializer: Option<TdPyCallable>,
}

#[pymethods]
impl ManualInputConfig {
    #[new]
    #[args(input_builder, deserializer)]
    fn new(input_builder: Py<PyAny>, deserializer: Option<TdPyCallable>) -> (Self, InputConfig) {
        (
            Self {
                input_builder,
                deserializer,
            },
            InputConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self, py: Python) -> (&str, Py<PyAny>, Option<TdPyCallable>) {
        (
            "ManualInputConfig",
            self.input_builder.clone_ref(py),
            self.deserializer.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str, Option<TdPyCallable>) {
        (&"", None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualInputConfig", input_builder, deserializer)) = state.extract() {
            self.input_builder = input_builder;
            self.deserializer = deserializer;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualInputConfig: {state:?}"
            )))
        }
    }
}

#[pyclass(module = "bytewax.inputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct InputPartitionerConfig;

#[pyclass(module = "bytewax.inputs", extends = InputPartitionerConfig)]
#[pyo3(text_signature = "(messages_per_epoch)")]
pub(crate) struct BatchInputPartitionerConfig {
    pub messages_per_epoch: u64,
}

#[pymethods]
impl BatchInputPartitionerConfig {
    #[new]
    #[args(messages_per_epoch = 1)]
    fn new(messages_per_epoch: u64) -> (Self, InputPartitionerConfig) {
        (Self { messages_per_epoch }, InputPartitionerConfig {})
    }

    fn __getstate__(&self) -> (&str, u64) {
        ("BatchInputPartitionerConfig", self.messages_per_epoch)
    }

    fn __getnewargs__(&self) -> (u64,) {
        (1,)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("BatchInputPartitionerConfig", messages_per_epoch)) = state.extract() {
            self.messages_per_epoch = messages_per_epoch;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for BatchInputPartitionerConfig: {state:?}"
            )))
        }
    }
}

#[pyclass(module = "bytewax.inputs", extends = InputPartitionerConfig)]
//TODO is this the right text signature?
#[pyo3(text_signature = "(window_length, window_start_time = None, time_gettter = None, epoch_start = 0)")]
pub(crate) struct TumblingWindowInputPartitionerConfig {
    window_length: PyDuration,
    window_start_time: Option<PyNaiveDateTime>,
    time_getter: Option<TdPyCallable>,
    epoch_start: u64,
}

#[pymethods]
impl TumblingWindowInputPartitionerConfig {
    #[new]
    #[args(
        window_length,
        window_start_time = "None",
        time_getter = "None",
        epoch_start = 0
    )]
    fn new(
        window_length: PyDuration,
        window_start_time: Option<PyNaiveDateTime>,
        time_getter: Option<TdPyCallable>,
        epoch_start: u64,
    ) -> (Self, InputPartitionerConfig) {
        (
            Self {
                window_length,
                window_start_time,
                time_getter,
                epoch_start,
            },
            InputPartitionerConfig {},
        )
    }

    fn __getstate__(
        &self,
    ) -> (
        &str,
        Option<pyo3_chrono::NaiveDateTime>,
        pyo3_chrono::Duration,
        Option<TdPyCallable>,
        u64,
    ) {
        (
            "TumblingWindowInputPartitionerConfig",
            self.window_start_time.clone(),
            self.window_length.clone(),
            self.time_getter.clone(),
            self.epoch_start,
        )
    }

    fn __getnewargs__(
        &self,
    ) -> (
        pyo3_chrono::Duration,
        Option<pyo3_chrono::NaiveDateTime>,
        Option<TdPyCallable>,
        u64,
    ) {
        let dur: pyo3_chrono::Duration = chrono::Duration::minutes(5).into();
        (dur, None, None, 1)
    }

    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok((
            "TumblingWindowInputPartitionerConfig",
            window_length,
            window_start_time,
            time_getter,
            epoch_start,
        )) = state.extract()
        {
            self.window_length = window_length;
            self.window_start_time = window_start_time;
            self.time_getter = time_getter;
            self.epoch_start = epoch_start;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TumblingWindowInputPartitionerConfig: {state:?}"
            )))
        }
    }
}

/// Take a single data element or timestamp and feed it into dataflow
/// Pump will be called in the worker's "main" loop to feed data in.
pub trait InputIter {
    fn next(&mut self) -> Option<Py<PyAny>>;
    fn empty(&mut self) -> bool;
}

pub trait InputManager {
    fn pump(&mut self);
    fn input_time(&mut self) -> &u64;
    fn input_remains(&mut self) -> bool;
}

/// Encapsulates the process of pulling data out of a Kafka
/// stream and feeding it into Timely.
struct KafkaIter {
    consumer: KafkaConsumer,
    deserializer: Option<TdPyCallable>,
}

impl KafkaIter {
    fn new(config: PyRef<KafkaInputConfig>) -> Self {
        let deserializer = config.deserializer.clone();
        let consumer = KafkaConsumer::new(config);
        Self {
            consumer,
            deserializer,
        }
    }
}

impl InputIter for KafkaIter {
    fn next(&mut self) -> Option<Py<PyAny>> {
        match self.consumer.next() {
            None => None,
            Some(msg) => {
                //TODO ultimately we want the entire message, prob. But for now v. expensive
                Python::with_gil(|py| {
                    let payload: Py<PyAny> = msg
                        .payload()
                        .map_or(py.None(), |k| PyBytes::new(py, k).into());
                    let payload: Py<PyAny> = (payload).into_py(py);

                    if let Some(deserializer) = &self.deserializer {
                        let deserialized_res = deserializer.call1(py, (payload,));
                        match deserialized_res {
                            Ok(deserialized) => {
                                if deserialized.is_none(py) {
                                    Some(deserialized)
                                } else {
                                    Some((deserialized,).into_py(py))
                                }
                            }
                            Err(err) => {
                                panic!("Could not deserialize {:?}", err);
                            }
                        }
                    } else {
                        Some(payload)
                    }
                })
            }
        }
    }
    fn empty(&mut self) -> bool {
        return false;
    }
}

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
struct ManualIter {
    pull_from_pyiter: TdPyIterator,
    empty: bool,
    deserializer: Option<TdPyCallable>,
}
impl ManualIter {
    fn new(
        py: Python,
        worker_index: usize,
        worker_count: usize,
        resume_epoch: u64,
        config: PyRef<ManualInputConfig>,
    ) -> Self {
        let worker_input: TdPyIterator = config
            .input_builder
            .call1(py, (worker_index, worker_count, resume_epoch))
            .unwrap()
            .extract(py)
            .unwrap();
        Self {
            pull_from_pyiter: worker_input,
            empty: false,
            deserializer: config.deserializer.clone(),
        }
    }
}

impl InputIter for ManualIter {
    fn next(&mut self) -> Option<Py<PyAny>> {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            match pull_from_pyiter.next() {
                Some(result) => match result {
                    Ok(item) => {
                        if let Some(deserializer) = &self.deserializer {
                            let deserialized_res = deserializer.call1(py, (item,));
                            match deserialized_res {
                                Ok(deserialized) => {
                                    let for_timely: Py<PyAny> = deserialized.into_py(py);
                                    Some(for_timely)
                                }
                                Err(err) => {
                                    panic!("Could not deserialize {:?}", err);
                                }
                            }
                        } else {
                            let for_timely: Py<PyAny> = item.into_py(py);
                            Some(for_timely)
                        }
                    }
                    Err(err) => {
                        std::panic::panic_any(err);
                    }
                },
                None => {
                    self.empty = true;
                    None
                }
            }
        })
    }
    fn empty(&mut self) -> bool {
        return self.empty;
    }
}

struct Batcher {
    input_iter: Box<dyn InputIter>,
    push_to_timely: InputHandle<u64, TdPyAny>,
    current_epoch: u64,
    desired_messages_per_epoch: u64,
    current_messages_per_epoch: u64,
}

impl Batcher {
    fn new(
        input_iter: Box<dyn InputIter>,
        config: Option<PyRef<BatchInputPartitionerConfig>>,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        match config {
            Some(config) => { 
                Self {
                    input_iter,
                    push_to_timely,
                    current_epoch: 0,
                    desired_messages_per_epoch: config.messages_per_epoch,
                    current_messages_per_epoch: 0,
                }
            },
            None => {
                Self {
                    input_iter,
                    push_to_timely,
                    current_epoch: 0,
                    desired_messages_per_epoch: 1,
                    current_messages_per_epoch: 0,
                } 
            }
        }

    }
}

impl InputManager for Batcher {
    fn pump(&mut self) {
        if self.current_messages_per_epoch == self.desired_messages_per_epoch {
            self.current_messages_per_epoch = 0;
            self.current_epoch += 1;
            self.push_to_timely.advance_to(self.current_epoch);
        } else {
            match self.input_iter.next() {
                Some(r) => {
                    self.current_messages_per_epoch += 1;
                    self.push_to_timely.send(r.into())
                }
                None => {
                    self.current_messages_per_epoch = 0;
                    self.current_epoch += 1;
                    self.push_to_timely.advance_to(self.current_epoch);
                }
            }
        }
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.input_iter.empty()
    }
}

struct Tumbler {
    input_iter: Box<dyn InputIter>,
    push_to_timely: InputHandle<u64, TdPyAny>,
    current_epoch: u64,
    window_length: chrono::Duration,
    time_getter: Option<TdPyCallable>,
    current_window_end: Option<chrono::DateTime<Utc>>,
    window_start_time: Option<chrono::DateTime<Utc>>,
}

impl Tumbler {
    fn new(
        input_iter: Box<dyn InputIter>,
        config: PyRef<TumblingWindowInputPartitionerConfig>,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        let time_getter = config.time_getter.clone();
        let ws: chrono::NaiveDateTime = config.window_start_time.clone().unwrap().into();
        let wse = chrono::DateTime::<Utc>::from_utc(ws, Utc);
        let wl: chrono::Duration = config.window_length.into();
        Self {
            input_iter: input_iter,
            push_to_timely,
            current_epoch: config.epoch_start,
            time_getter: time_getter,
            window_length: wl,
            current_window_end: Some(wse + wl),
            window_start_time: Some(wse),
        }
    }
}

fn get_time(time_getter: TdPyCallable, item: Py<PyAny>) -> chrono::DateTime<Utc> {
    Python::with_gil(|py| {
        let f = time_getter.call1(py, (item,)).unwrap();
        let t: PyNaiveDateTime = f.extract(py).unwrap();
        let chrono_t: chrono::NaiveDateTime = t.into();
        let item_time: chrono::DateTime<Utc> = chrono::DateTime::<Utc>::from_utc(chrono_t, Utc);
        return item_time;
    })
}

impl InputManager for Tumbler {
    fn pump(&mut self) {
        // move time fetching to iter, return struct
        // receives tuples of item, time? Or maybe a struct
        //Event time
        if let Some(time_getter) = &self.time_getter {
            loop {
                if self.input_iter.empty() {
                    break;
                }
                if let Some(item) = self.input_iter.next() {
                    let getter = time_getter.clone();
                    let item_for_time = item.clone();
                    let item_time = get_time(getter, item_for_time);
                    // sort to buffer
                    if self.window_start_time.is_none() {
                        dbg!("Setting window start");
                        self.window_start_time = Some(item_time);
                        self.current_window_end = Some(item_time + self.window_length);
                    }
                    if item_time >= self.current_window_end.unwrap() {
                        dbg!("Window closing");
                        self.current_epoch += 1;
                        self.push_to_timely.advance_to(self.current_epoch);
                        self.current_window_end =
                            Some(self.current_window_end.unwrap() + self.window_length);
                    } else {
                        self.push_to_timely.send(item.into());
                        break;
                    }
                }
            }
        } else {
            //Process time
            let now = Utc::now();
            let end: chrono::DateTime<Utc> = self.current_window_end.unwrap();
            if now >= self.current_window_end.unwrap() {
                //reset window
                self.current_epoch += 1;
                self.push_to_timely.advance_to(self.current_epoch);
                self.current_window_end = Some(end + self.window_length);
            } else {
                loop {
                    if self.input_iter.empty() {
                        break;
                    }
                    match self.input_iter.next() {
                        Some(r) => {
                            self.push_to_timely.send(r.into());
                            break;
                        }
                        None => {
                            //TODO this message
                            dbg!("No messages available, trying again");
                        }
                    }
                }
            }
        }
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.input_iter.empty()
    }

    // fn update_windows(&mut self){}
}

pub(crate) fn input_partitioner_from_config(
    py: Python,
    partition_config: Option<Py<InputPartitionerConfig>>,
    input_config: Py<InputConfig>,
    input_handle: InputHandle<u64, TdPyAny>,
    worker_index: usize,
    worker_count: usize,
    resume_epoch: u64,
) -> Box<dyn InputManager> {
    let input_iter =
        input_iter_from_config(py, input_config, worker_index, worker_count, resume_epoch);
    match partition_config {
        Some(partition_config) => {
            let partition_config = partition_config.as_ref(py);
            if let Ok(batch_config) =
                partition_config.downcast::<PyCell<BatchInputPartitionerConfig>>()
            {
                let batch_config = batch_config.borrow();
                Box::new(Batcher::new(input_iter, Some(batch_config), input_handle))
            } else if let Ok(tumbler_config) =
                partition_config.downcast::<PyCell<TumblingWindowInputPartitionerConfig>>()
            {
                let tumbler_config = tumbler_config.borrow();
                Box::new(Tumbler::new(input_iter, tumbler_config, input_handle))
            } else {
                let pytype = partition_config.get_type();
                panic!("Unknown partition_config type: {pytype}")
            }
        }
        None => {
            Box::new(Batcher::new(input_iter, None, input_handle))
        }
    }
}

pub(crate) fn input_iter_from_config(
    py: Python,
    input_config: Py<InputConfig>,
    worker_index: usize,
    worker_count: usize,
    resume_epoch: u64,
) -> Box<dyn InputIter> {
    let input_config = input_config.as_ref(py);
    if let Ok(kafka_config) = input_config.downcast::<PyCell<KafkaInputConfig>>() {
        let kafka_config = kafka_config.borrow();
        Box::new(KafkaIter::new(kafka_config))
    } else if let Ok(manual_config) = input_config.downcast::<PyCell<ManualInputConfig>>() {
        let manual_config = manual_config.borrow();
        Box::new(ManualIter::new(
            py,
            worker_index,
            worker_count,
            resume_epoch,
            manual_config,
        ))
    } else {
        let pytype = input_config.get_type();
        panic!("Unknown input_config type: {pytype}")
    }
}

struct KafkaConsumer {
    rt: Runtime,
    consumer: BaseConsumer<CustomContext>,
}

impl KafkaConsumer {
    pub(crate) fn new(config: PyRef<KafkaInputConfig>) -> Self {
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

        Self { rt, consumer }
    }

    pub fn next(&mut self) -> Option<BorrowedMessage> {
        self.rt.block_on(async {
            match self.consumer.poll(StdDuration::from_millis(5000)) {
                None => None,
                Some(r) => match r {
                    Err(e) => panic!("Kafka error! {}", e),
                    Ok(s) => Some(s),
                },
            }
        })
    }
}

//  // A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// We're not using this yet, but seems like it will be helpful with recovery callbacks
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<InputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    m.add_class::<InputPartitionerConfig>()?;
    m.add_class::<TumblingWindowInputPartitionerConfig>()?;
    m.add_class::<BatchInputPartitionerConfig>()?;
    Ok(())
}
