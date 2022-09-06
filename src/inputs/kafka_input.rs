use std::collections::HashMap;
use std::task::Poll;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};

use serde::{Deserialize, Serialize};

use crate::pyo3_extensions::TdPyAny;
use crate::recovery::StateBytes;

use super::{distribute, InputConfig, InputReader};

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
/// Any additional configuration properties can be passed as kwargs. Note
/// that consumer group settings will be ignored.
/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
/// for more options.
///
/// Returns:
///
///   Config object. Pass this as the `input_config` argument to the
///   `bytewax.dataflow.Dataflow.input`.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(brokers, topic, tail, starting_offset, additional_configs='**')")]
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
    pub(crate) additional_configs: Option<HashMap<String, String>>,
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(
        brokers,
        topic,
        tail = true,
        starting_offset = "\"beginning\".to_string()",
        additional_configs = "**"
    )]
    fn new(
        brokers: Vec<String>,
        topic: String,
        tail: bool,
        starting_offset: String,
        additional_configs: Option<HashMap<String, String>>,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                topic,
                tail,
                starting_offset,
                additional_configs
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String, bool, String, Option<HashMap<String, String>>) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.topic.clone(),
            self.tail,
            self.starting_offset.clone(),
            self.additional_configs.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, bool, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, false, s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaInputConfig", brokers, topic, tail, starting_offset, additional_configs)) = state.extract() {
            self.brokers = brokers;
            self.topic = topic;
            self.tail = tail;
            self.starting_offset = starting_offset;
            self.additional_configs = additional_configs;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaInputConfig: {state:?}"
            )))
        }
    }
}

/// Read from Kafka for an input source.
// Using an rdkafka::admin::AdminClient did not always return partition counts
// for a topic, so create a BaseConsumer to fetch the metadata for a topic instead.
// Inspired by https://github.com/fede1024/rust-rdkafka/blob/5d23e82a675d9df1bf343aedcaa35be864787dab/tests/test_admin.rs#L33
fn get_kafka_partition_count(consumer: &BaseConsumer, topic: &str) -> i32 {
    let timeout = Some(Duration::from_secs(5));

    log::debug!("Attempting to fetch metadata from {}", topic);
    
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
    pub(crate) fn new(
        brokers: &[String],
        topic: &str,
        tail: bool,
        starting_offset: Offset,
        additional_configs: &Option<HashMap<String, String>>,
        worker_index: usize,
        worker_count: usize,
        resume_state_bytes: Option<StateBytes>,
    ) -> Self {
        let mut positions: HashMap<KafkaPartition, KafkaPosition> = resume_state_bytes
            .map(|resume_state_bytes| resume_state_bytes.de())
            .unwrap_or_default();

        let eof = !tail; 
        
        let mut config = ClientConfig::new();
        
        config
            .set("group.id", "BYTEWAX_IGNORED")
            .set("enable.auto.commit", "false")
            .set("bootstrap.servers", brokers.join(","))
            .set("enable.partition.eof", format!("{eof}"));

        if let Some(args) = additional_configs {
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
        for partition in distribute(0..partition_count, worker_index, worker_count) {
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
