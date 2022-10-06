use std::fmt::Debug;
use std::marker::PhantomData;

use log::debug;
use log::trace;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::BaseRecord;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use rdkafka::{Message, Offset, TopicPartitionList};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::ProgressRecoveryKey;
use super::{
    ProgressOp, ProgressReader, ProgressUpdate, ProgressWriter, RecoveryConfig, StateCollector,
    StateOp, StateReader, StateRecoveryKey, StateUpdate, StateWriter,
};

/// Use [Kafka](https://kafka.apache.org/) to store recovery data.
///
/// Uses a "progress" topic and a "state" topic with a number of
/// partitions equal to the number of workers. Will take advantage of
/// log compaction so that topic size is proportional to state size,
/// not epoch count.
///
/// Use a distinct topic prefix per dataflow so recovery data is not
/// mixed.
///
/// >>> from bytewax.execution import run_main
/// >>> from bytewax.inputs import TestingInputConfig
/// >>> from bytewax.outputs import StdOutputConfig
/// >>> flow = Dataflow()
/// >>> flow.inp("inp", TestingInputConfig(range(3)))
/// >>> flow.capture(StdOutputConfig())
/// >>> recovery_config = KafkaRecoveryConfig(
/// ...     ["localhost:9092"],
/// ...     "sample-dataflow",
/// ... )
/// >>> run_main(
/// ...     flow,
/// ...     recovery_config=recovery_config,
/// ... )  # doctest: +ELLIPSIS
/// (...)
///
/// If there's no previous recovery data, topics will automatically be
/// created with the correct number of partitions and log compaction
/// enabled
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic_prefix (str): Prefix used for naming topics. Must be
///       distinct per-dataflow. Two topics will be created using
///       this prefix `"topic_prefix-progress"` and
///       `"topic_prefix-state"`.
///
/// Returns:
///
///   Config object. Pass this as the `recovery_config` argument to
///   your execution entry point.
#[pyclass(module="bytewax.recovery", extends=RecoveryConfig)]
#[pyo3(text_signature = "(brokers, topic_prefix)")]
pub(crate) struct KafkaRecoveryConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic_prefix: String,
}

#[pymethods]
impl KafkaRecoveryConfig {
    #[new]
    #[args(brokers, topic_prefix)]
    pub(crate) fn new(brokers: Vec<String>, topic_prefix: String) -> (Self, RecoveryConfig) {
        (
            Self {
                brokers,
                topic_prefix,
            },
            RecoveryConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, Vec<String>, &str) {
        (
            "KafkaRecoveryConfig",
            self.brokers.clone(),
            &self.topic_prefix,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        (vec![], "UNINIT_PICKLED_STRING")
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaRecoveryConfig", hosts, topic_prefix)) = state.extract() {
            self.brokers = hosts;
            self.topic_prefix = topic_prefix;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaRecoveryConfig: {state:?}"
            )))
        }
    }
}

impl KafkaRecoveryConfig {
    pub(crate) fn progress_topic(&self) -> String {
        format!("{}-progress", self.topic_prefix)
    }

    pub(crate) fn state_topic(&self) -> String {
        format!("{}-state", self.topic_prefix)
    }
}

pub(crate) fn create_kafka_topic(brokers: &[String], topic: &str, partitions: i32) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let admin: AdminClient<_> = rt.block_on(async {
        ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .expect("Error building Kafka admin")
    });
    let admin_options = AdminOptions::new();

    let new_topic = NewTopic {
        name: topic,
        num_partitions: partitions,
        // I believe this chooses the default replication factor.
        replication: TopicReplication::Fixed(-1),
        config: vec![("cleanup.policy", "compact")],
    };
    let future = admin.create_topics(vec![&new_topic], &admin_options);
    let result = rt
        .block_on(future)
        .expect("Error calling create Kafka topic on admin")
        .pop()
        .unwrap();
    match result {
        Ok(topic) => {
            debug!("Created Kafka topic={topic:?}");
        }
        Err((topic, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists)) => {
            debug!("Kafka topic={topic:?} already exists; continuing");
        }
        Err((topic, err_code)) => {
            panic!("Error creating Kafka topic={topic:?}: {err_code:?}")
        }
    }
}

fn to_bytes<T>(obj: &T) -> Vec<u8>
where
    T: Serialize,
{
    // TODO: Figure out if there's a more robust-to-evolution way
    // to serialize this key. If the serialization changes between
    // versions, then recovery doesn't work. Or if we use an
    // encoding that isn't deterministic.
    bincode::serialize(obj).expect("Error serializing Kafka recovery data")
}

fn from_bytes<'a, T>(bytes: &'a [u8]) -> T
where
    T: Deserialize<'a>,
{
    let t_name = std::any::type_name::<T>();
    bincode::deserialize(bytes)
        .unwrap_or_else(|_| panic!("Error deserializing Kafka recovery data {t_name})"))
}

/// This is a generic wrapper around [`BaseProducer`] which adds
/// serde and only writes to a single topic and partition.
pub(crate) struct KafkaWriter<K, P> {
    producer: BaseProducer,
    topic: String,
    partition: i32,
    key_type: PhantomData<K>,
    payload_type: PhantomData<P>,
}

impl<K: Serialize, P: Serialize> KafkaWriter<K, P> {
    pub(crate) fn new(brokers: &[String], topic: String, partition: i32) -> Self {
        debug!("Creating Kafka producer with brokers={brokers:?} topic={topic:?}");
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .expect("Error building Kafka producer");

        Self {
            producer,
            topic,
            partition,
            key_type: PhantomData,
            payload_type: PhantomData,
        }
    }

    fn write(&self, key: &K, payload: &P) {
        let key_bytes = to_bytes(key);
        let payload_bytes = to_bytes(payload);
        let record = BaseRecord::to(&self.topic)
            .key(&key_bytes)
            .payload(&payload_bytes)
            .partition(self.partition);

        self.producer.send(record).expect("Error writing state");
        self.producer.poll(Timeout::Never);
    }

    fn delete(&self, key: &K) {
        let key_bytes = to_bytes(key);
        let record = BaseRecord::<Vec<u8>, Vec<u8>>::to(&self.topic)
            .key(&key_bytes)
            .partition(self.partition);
        // Explicitly no payload to mark as delete for key.

        self.producer.send(record).expect("Error deleting state");
        self.producer.poll(Timeout::Never);
    }
}

impl<T> StateWriter<T> for KafkaWriter<StateRecoveryKey<T>, StateOp>
where
    T: Serialize + Debug,
{
    fn write(&mut self, update: &StateUpdate<T>) {
        let StateUpdate(key, op) = update;
        KafkaWriter::write(self, key, op);
        trace!("Wrote state update {update:?}");
    }
}

impl<T> StateCollector<T> for KafkaWriter<StateRecoveryKey<T>, StateOp>
where
    T: Serialize + Debug,
{
    fn delete(&mut self, key: &StateRecoveryKey<T>) {
        KafkaWriter::delete(self, key);
        trace!("Deleted state for {key:?}");
    }
}

/// This is a generic wrapper around [`BaseConsumer`] which adds
/// serde and reads from only a single topic and partition.
pub(crate) struct KafkaReader<K, P> {
    consumer: BaseConsumer,
    key_type: PhantomData<K>,
    payload_type: PhantomData<P>,
}

impl<K: DeserializeOwned, P: DeserializeOwned> KafkaReader<K, P> {
    pub(crate) fn new(brokers: &[String], topic: &str, partition: i32) -> Self {
        debug!("Loading recovery data from brokers={brokers:?} topic={topic:?}");
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            // We don't want to use consumer groups because
            // re-balancing makes no sense in the recovery
            // context. librdkafka requires you to set a consumer
            // group, though, but they say it is never used if you
            // don't call
            // `subscribe`. https://github.com/edenhill/librdkafka/issues/593#issuecomment-278954990
            .set("group.id", "BYTEWAX_IGNORED")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "true")
            .create()
            .expect("Error building Kafka consumer");

        let mut partitions = TopicPartitionList::new();
        partitions
            .add_partition_offset(topic, partition, Offset::Beginning)
            .unwrap();
        consumer
            .assign(&partitions)
            .expect("Error assigning Kafka recovery topic");

        Self {
            consumer,
            key_type: PhantomData,
            payload_type: PhantomData,
        }
    }

    fn read(&mut self) -> Option<(Option<K>, Option<P>)> {
        let msg_result = self.consumer.poll(Timeout::Never);
        match msg_result {
            Some(Ok(msg)) => Some((msg.key().map(from_bytes), msg.payload().map(from_bytes))),
            Some(Err(KafkaError::PartitionEOF(_))) => None,
            Some(Err(err)) => panic!("Error reading from Kafka topic: {err:?}"),
            None => None,
        }
    }
}

impl<T> StateReader<T> for KafkaReader<StateRecoveryKey<T>, StateOp>
where
    T: DeserializeOwned + Debug,
{
    fn read(&mut self) -> Option<StateUpdate<T>> {
        loop {
            match KafkaReader::read(self) {
                // Skip deletions if they haven't been compacted.
                Some((_, None)) => continue,
                Some((Some(key), Some(op))) => {
                    let update = StateUpdate(key, op);
                    trace!("Read state update {update:?}");
                    return Some(update);
                }
                Some((None, _)) => panic!("Missing key in reading state Kafka topic"),
                None => return None,
            }
        }
    }
}

impl<T> ProgressWriter<T> for KafkaWriter<ProgressRecoveryKey, ProgressOp<T>>
where
    T: Serialize + Debug,
{
    fn write(&mut self, update: &ProgressUpdate<T>) {
        let ProgressUpdate(key, op) = update;
        KafkaWriter::write(self, key, op);
        trace!("Wrote progress update {update:?}");
    }
}

impl<T> ProgressReader<T> for KafkaReader<ProgressRecoveryKey, ProgressOp<T>>
where
    T: DeserializeOwned + Debug,
{
    fn read(&mut self) -> Option<ProgressUpdate<T>> {
        match KafkaReader::read(self) {
            Some((Some(key), Some(op))) => {
                let update = ProgressUpdate(key, op);
                trace!("Read progress update {update:?}");
                Some(update)
            }
            None => None,
            _ => panic!("Missing key or value in reading recovery progress Kafka topic"),
        }
    }
}
