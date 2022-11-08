use std::collections::HashMap;
use std::task::Poll;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};

use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};

use crate::common::StringResult;
use crate::execution::WorkerIndex;
use crate::pickle_extract;
use crate::pyo3_extensions::TdPyAny;
use crate::recovery::model::StateBytes;

use super::{distribute, InputBuilder, InputConfig, InputReader};

/// Use [Kafka](https://kafka.apache.org) as the input
/// source.
///
/// Kafka messages will be passed through the dataflow as two-tuples
/// of `(key_bytes, payload_bytes)`.
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic (str): Topic to which consumer will subscribe.
///
///   tail (bool): Wait for new data on this topic when the end is
///       initially reached.
///
///   starting_offset (str): Can be "beginning" or "end". Delegates
///       where to resume if auto_commit is not enabled. Defaults to
///       "beginning".
///
///   additional_properties (dict): Any additional configuration properties.
///       Note that consumer group settings will be ignored.
///       See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
///       for more options.
///
/// Returns:
///
///   Config object. Pass this as the `input_config` argument of the
///   `bytewax.dataflow.Dataflow.input` operator.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(brokers, topic, tail, starting_offset, additional_properties)")]
#[derive(Clone)]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic: String,
    #[pyo3(get)]
    pub(crate) tail: bool,
    #[pyo3(get)]
    pub(crate) starting_offset: String,
    #[pyo3(get)]
    pub(crate) additional_properties: Option<HashMap<String, String>>,
}

impl InputBuilder for KafkaInputConfig {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: usize,
        resume_snapshot: Option<StateBytes>,
    ) -> StringResult<Box<dyn InputReader<TdPyAny>>> {
        let starting_offset = match self.starting_offset.as_str() {
            "beginning" => Ok(Offset::Beginning),
            "end" => Ok(Offset::End),
            unk => Err(format!(
                "starting_offset should be either `\"beginning\"` or `\"end\"`; got `{unk:?}`"
            )),
        }?;
        let reader = py.allow_threads(|| {
            SendWrapper::new(KafkaInput::new(
                &self.brokers,
                &self.topic,
                self.tail,
                starting_offset,
                &self.additional_properties,
                worker_index,
                worker_count,
                resume_snapshot,
            ))
        });

        Ok(Box::new(reader.take()))
    }
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(
        brokers,
        topic,
        tail = true,
        starting_offset = "\"beginning\".to_string()",
        additional_properties = "None"
    )]
    fn new(
        brokers: Vec<String>,
        topic: String,
        tail: bool,
        starting_offset: String,
        additional_properties: Option<HashMap<String, String>>,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                topic,
                tail,
                starting_offset,
                additional_properties,
            },
            InputConfig {},
        )
    }
    /// Pickle as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "KafkaInputConfig".into_py(py)),
                ("brokers", self.brokers.clone().into_py(py)),
                ("topic", self.topic.clone().into_py(py)),
                ("tail", self.tail.into_py(py)),
                ("starting_offset", self.starting_offset.clone().into_py(py)),
                (
                    "additional_properties",
                    self.additional_properties.clone().into_py(py),
                ),
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, bool, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, false, s)
    }

    /// Unpickle from a PyDict of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        pickle_extract!(self, dict, brokers);
        pickle_extract!(self, dict, topic);
        pickle_extract!(self, dict, tail);
        pickle_extract!(self, dict, starting_offset);
        pickle_extract!(self, dict, additional_properties);                                
        Ok(())
    }
}

/// Read from Kafka for an input source.
// Using an rdkafka::admin::AdminClient did not always return partition counts
// for a topic, so create a BaseConsumer to fetch the metadata for a topic instead.
// Inspired by https://github.com/fede1024/rust-rdkafka/blob/5d23e82a675d9df1bf343aedcaa35be864787dab/tests/test_admin.rs#L33
fn get_kafka_partition_count(consumer: &BaseConsumer, topic: &str) -> i32 {
    let timeout = Some(Duration::from_secs(5));

    tracing::debug!("Attempting to fetch metadata from {}", topic);

    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| e.to_string())
        .expect("Unable to fetch topic metadata for {topic}");

    let user_topic = &metadata.topics()[0];
    user_topic
        .partitions()
        .len()
        .try_into()
        .expect("Unable to convert topic partition count to i32")
}

/// Wrapper struct representing a Kafka partition ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct KafkaPartition(i32);

/// Wrapper struct representing the current Kafka consumer position
/// for a partition.
///
/// This is different from [`rdkafka`]'s [`Offset`] to implement
/// serde.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum KafkaPosition {
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

pub(crate) struct KafkaInput {
    consumer: BaseConsumer,
    positions: HashMap<KafkaPartition, KafkaPosition>,
}

impl KafkaInput {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        brokers: &[String],
        topic: &str,
        tail: bool,
        starting_offset: Offset,
        additional_properties: &Option<HashMap<String, String>>,
        worker_index: WorkerIndex,
        worker_count: usize,
        resume_snapshot: Option<StateBytes>,
    ) -> Self {
        let mut positions = resume_snapshot
            .map(StateBytes::de::<HashMap<KafkaPartition, KafkaPosition>>)
            .unwrap_or_default();

        let eof = !tail;

        let mut config = ClientConfig::new();

        config
            .set("group.id", "BYTEWAX_IGNORED")
            .set("enable.auto.commit", "false")
            .set("bootstrap.servers", brokers.join(","))
            .set("enable.partition.eof", format!("{eof}"));

        if let Some(args) = additional_properties {
            for (key, value) in args.iter() {
                config.set(key, value);
            }
        }

        let consumer: BaseConsumer = config
            .create()
            .expect("Error building input Kafka consumer");

        let partition_count = get_kafka_partition_count(&consumer, topic);
        if partition_count == 0 {
            panic!("Topic {} does not exist, please create first", topic);
        }

        let mut partitions = TopicPartitionList::new();
        for partition in distribute(0..partition_count, worker_index.0, worker_count) {
            let partition = KafkaPartition(partition);
            let resume_offset: Option<Offset> =
                (*positions.entry(partition).or_insert(KafkaPosition::Default)).into();
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
    #[tracing::instrument(name = "kafka_input", level = "trace", skip_all)]
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

    #[tracing::instrument(name = "kafka_input_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<HashMap<KafkaPartition, KafkaPosition>>(&self.positions)
    }
}
