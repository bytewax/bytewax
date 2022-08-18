//! Internal code for input systems.
//!
//! For a user-centric version of input, read the `bytewax.input`
//! Python module docstring. Read that first.
//!
//! Architecture
//! ------------
//!
//! The one extra quirk here is that input is completely decoupled
//! from epoch generation. See [`crate::execution`] for how Timely
//! sources are generated and epochs assigned. The only goal of the
//! input system is "what's my next item for this worker?"
//!
//! Output is based around the core trait of [`InputReader`].  The
//! [`crate::dataflow::Dataflow::input`] operator delegates to impls
//! of that trait for actual writing.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`KafkaInputConfig`] represents a token in Python for
//! how to create a [`KafkaInputReader`].

use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyCoroIterator};
use crate::recovery::StateBytes;
use crate::with_traceback;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
/// Because Bytewax's execution is cooperative, the resulting
/// iterators _must not block_ waiting for new data, otherwise pending
/// execution of other steps in the dataflow will be delayed an
/// throughput will be reduced. If you are using a generator and no
/// data is ready yet, have it `yield None` or just `yield` to signal
/// this.
///
/// Args:
///
///     input_builder: `input_builder(worker_index: int, worker_count:
///         int, resume_state: Option[Any]) => Iterator[Tuple[Any,
///         Any]]` Builder function which returns an iterator of
///         2-tuples of `(state, item)`. `item` is the input that
///         worker should introduce into the dataflow. `state` is a
///         snapshot of any internal state it will take to resume this
///         input from its current position _after the current
///         item_. Note that e.g. returning the same list from each
///         worker will result in duplicate data in the dataflow.
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to the
///     `bytewax.dataflow.Dataflow.input`.
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
/// Args:
///
///     brokers (List[str]): List of `host:port` strings of Kafka
///         brokers.
///
///     topic (str): Topic to which consumer will subscribe.
///
///     tail (bool): Wait for new data on this topic when the end is
///         initially reached.
///
///     starting_offset (str): Can be "beginning" or "end". Delegates
///         where to resume if auto_commit is not enabled. Defaults to
///         "beginning".
///
/// Returns:
///
///     Config object. Pass this as the `input_config` argument to the
///     `bytewax.dataflow.Dataflow.input`.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(brokers, topic, tail, starting_offset)")]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    brokers: Vec<String>,
    #[pyo3(get)]
    topic: String,
    #[pyo3(get)]
    tail: bool,
    #[pyo3(get)]
    starting_offset: String,
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(
        brokers,
        topic,
        tail = true,
        starting_offset = "\"beginning\".to_string()"
    )]
    fn new(
        brokers: Vec<String>,
        topic: String,
        tail: bool,
        starting_offset: String,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                topic,
                tail,
                starting_offset,
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String, bool, String) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.topic.clone(),
            self.tail,
            self.starting_offset.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, bool, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, false, s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaInputConfig", brokers, topic, tail, starting_offset)) = state.extract() {
            self.brokers = brokers;
            self.topic = topic;
            self.tail = tail;
            self.starting_offset = starting_offset;
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
    /// Return the next item from this input, if any.
    ///
    /// This method must _never block or wait_ on data. If there's no
    /// data yet, return [`Poll::Pending`].
    ///
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

    /// Snapshot the internal state of this input.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// input exactly how it is currently. This will probably mean
    /// writing out any offsets in the various input streams.
    fn snapshot(&self) -> StateBytes;
}

// TODO: Convert this to use the builder pattern to match the other
// stateful components.
pub(crate) fn build_input_reader(
    py: Python,
    config: Py<InputConfig>,
    worker_index: usize,
    worker_count: usize,
    resume_state_bytes: Option<StateBytes>,
) -> Result<Box<dyn InputReader<TdPyAny>>, String> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let config = config.as_ref(py);

    if let Ok(config) = config.downcast::<PyCell<ManualInputConfig>>() {
        let config = config.borrow();

        let input_builder = config.input_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let reader = ManualInput::new(
            py,
            input_builder,
            worker_index,
            worker_count,
            resume_state_bytes,
        );

        Ok(Box::new(reader))
    } else if let Ok(config) = config.downcast::<PyCell<KafkaInputConfig>>() {
        let config = config.borrow();

        let brokers = &config.brokers;
        let topic = &config.topic;
        let tail = config.tail;
        let starting_offset = match config.starting_offset.as_str() {
            "beginning" => Ok(Offset::Beginning),
            "end" => Ok(Offset::End),
            unk => Err(format!(
                "starting_offset should be either `\"beginning\"` or `\"end\"`; got `{unk:?}`"
            )),
        }?;

        let reader = py.allow_threads(|| {
            SendWrapper::new(KafkaInput::new(
                brokers,
                topic,
                tail,
                starting_offset,
                worker_index,
                worker_count,
                resume_state_bytes,
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
    pyiter: TdPyCoroIterator,
    last_state: TdPyAny,
}

impl ManualInput {
    fn new(
        py: Python,
        input_builder: TdPyCallable,
        worker_index: usize,
        worker_count: usize,
        resume_state_bytes: Option<StateBytes>,
    ) -> Self {
        let resume_state: TdPyAny = resume_state_bytes
            .map(|resume_state_bytes| resume_state_bytes.de())
            .unwrap_or_else(|| py.None().into());

        let pyiter: TdPyCoroIterator = with_traceback!(py, {
            input_builder
                .call1(py, (worker_index, worker_count, resume_state.clone_ref(py)))?
                .extract(py)
        });

        Self {
            pyiter,
            last_state: resume_state,
        }
    }
}

impl InputReader<TdPyAny> for ManualInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        self.pyiter.next().map(|poll| {
            poll.map(|state_item_pytuple| {
                Python::with_gil(|py| {
                    let (updated_state, item): (TdPyAny, TdPyAny) =
                        with_traceback!(py, {
                            state_item_pytuple
                                .extract(py)
                                .map_err(|_err| PyTypeError::new_err(format!("Manual input builders must yield `(state, item)` two-tuples; got `{state_item_pytuple:?}` instead")))
                        });

                    self.last_state = updated_state;

                    item
                })
            })
        })
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.last_state)
    }
}

/// Wrapper struct representing a Kafka partition ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct KafkaPartition(i32);

/// Wrapper struct representing the current Kafka consumer position
/// for a partition.
///
/// This is different from [`rdkafka`]'s [`Offset`] to implement
/// serde.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum KafkaPosition {
    /// We should resume from this offset.
    Offset(i64),
    /// We should resume from the end of the partition.
    End,
    /// We haven't read any data from this partition yet so use the
    /// default offset on resume.
    Default,
}

/// To bridge with [`rdkafka`]'s [`Offset`] type.
impl From<KafkaPosition> for Option<Offset> {
    fn from(src: KafkaPosition) -> Self {
        match src {
            KafkaPosition::Offset(offset) => Some(Offset::Offset(offset)),
            KafkaPosition::End => Some(Offset::End),
            KafkaPosition::Default => None,
        }
    }
}

/// Read from Kafka for an input source.
struct KafkaInput {
    consumer: BaseConsumer,
    positions: HashMap<KafkaPartition, KafkaPosition>,
}

// Using an rdkafka::admin::AdminClient did not always return partition counts
// for a topic, so create a BaseConsumer to fetch the metadata for a topic instead.
// Inspired by https://github.com/fede1024/rust-rdkafka/blob/5d23e82a675d9df1bf343aedcaa35be864787dab/tests/test_admin.rs#L33
fn get_kafka_partition_count(brokers: &[String], topic: &str) -> i32 {
    let consumer: BaseConsumer<DefaultConsumerContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers.join(","))
        .create()
        .expect("creating consumer failed");

    let timeout = Some(Duration::from_secs(5));

    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| e.to_string())
        .expect("Unable to fetch topic metadata for {topic}");
    let topic = &metadata.topics()[0];
    topic
        .partitions()
        .len()
        .try_into()
        .expect("Unable to convert topic partition count to i32")
}

// TODO: One day could use this impl for the Python version in
// `bytewax.inputs.distribute`? Hard to do because there's no PyO3
// bridging of `impl Iterator`.
fn distribute<T>(
    it: impl IntoIterator<Item = T>,
    index: usize,
    count: usize,
) -> impl Iterator<Item = T> {
    assert!(index < count);
    it.into_iter()
        .enumerate()
        .filter(move |(i, _x)| i % count == index)
        .map(|(_i, x)| x)
}

#[test]
fn test_distribute() {
    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 0, 2).collect();
    let expected = vec!["blue", "red"];
    assert_eq!(found, expected);

    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 1, 2).collect();
    let expected = vec!["green"];
    assert_eq!(found, expected);
}

#[test]
fn test_distribute_empty() {
    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 3, 5).collect();
    let expected: Vec<&str> = vec![];
    assert_eq!(found, expected);
}

impl KafkaInput {
    fn new(
        brokers: &[String],
        topic: &str,
        tail: bool,
        starting_offset: Offset,
        worker_index: usize,
        worker_count: usize,
        resume_state_bytes: Option<StateBytes>,
    ) -> Self {
        let mut positions: HashMap<KafkaPartition, KafkaPosition> = resume_state_bytes
            .map(|resume_state_bytes| resume_state_bytes.de())
            .unwrap_or_default();

        let eof = !tail;

        let consumer: BaseConsumer = ClientConfig::new()
            .set("group.id", "BYTEWAX_IGNORED")
            .set("enable.auto.commit", "false")
            .set("bootstrap.servers", brokers.join(","))
            .set("enable.partition.eof", format!("{eof}"))
            .create()
            .expect("Error building input Kafka consumer");

        let partition_count = get_kafka_partition_count(brokers, topic);
        let mut partitions = TopicPartitionList::new();
        for partition in distribute(0..partition_count, worker_index, worker_count) {
            let partition = KafkaPartition(partition);
            let resume_offset: Option<Offset> = positions
                .entry(partition)
                .or_insert(KafkaPosition::Default)
                .clone()
                .into();
            // If we don't know the offset for this partition from the
            // resume state, use the default starting offset.
            let offset = resume_offset.unwrap_or(starting_offset);
            partitions
                .add_partition_offset(topic, partition.0, offset)
                .unwrap();
        }
        consumer
            .assign(&partitions)
            .expect("Error assigning Kafka topic partitions");

        Self {
            consumer,
            positions,
        }
    }
}

impl InputReader<TdPyAny> for KafkaInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        match self.consumer.poll(Duration::from_millis(0)) {
            None => Poll::Pending,
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                let partition = KafkaPartition(partition);
                self.positions.insert(partition, KafkaPosition::End);

                // Only signal that this input source is done once all
                // partitions are done, not just one.
                if self
                    .positions
                    .values()
                    .all(|pos| *pos == KafkaPosition::End)
                {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                }
            }
            Some(Err(err)) => panic!("Error reading input from Kafka topic: {err:?}"),
            Some(Ok(msg)) => {
                let item: TdPyAny = Python::with_gil(|py| {
                    let key: Py<PyAny> =
                        msg.key().map_or(py.None(), |k| PyBytes::new(py, k).into());
                    let payload: Py<PyAny> = msg
                        .payload()
                        .map_or(py.None(), |k| PyBytes::new(py, k).into());
                    let key_payload: Py<PyAny> = (key, payload).into_py(py);
                    key_payload.into()
                });

                // TODO: Do we need to use self.consumer.positions()?
                // TODO: Do we need to handle partition addition and
                // removal?
                let partition = KafkaPartition(msg.partition());
                let position = KafkaPosition::Offset(msg.offset() + 1);
                self.positions.insert(partition, position);

                Poll::Ready(Some(item))
            }
        }
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.positions)
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<InputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    Ok(())
}
