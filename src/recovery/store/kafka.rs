//! Kafka implementation of state and progress stores.
//!
//! Generics are used to generate concrete versions of the reader and
//! writer for the relevant schema.

use std::fmt::Debug;
use std::marker::PhantomData;

use log::debug;
use log::trace;
use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::NewTopic;
use rdkafka::admin::TopicReplication;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::BaseRecord;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use rdkafka::{Message, Offset, TopicPartitionList};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::recovery::model::*;

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
    debug!("Creating Kafka topic={topic:?}");
    let result = rt
        .block_on(future)
        .expect("Error calling create Kafka topic on admin")
        .pop()
        .unwrap();
    match result {
        Ok(_topic) => {}
        Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
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

impl<K, P> KafkaWriter<K, P> {
    pub fn new(brokers: &[String], topic: String, partition: i32) -> Self {
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
}

impl<K, V> KWriter<K, V> for KafkaWriter<K, V>
where
    K: Serialize + Debug,
    V: Serialize + Debug,
{
    fn write(&mut self, kchange: KChange<K, V>) {
        trace!("Writing change {kchange:?}");
        let KChange(key, change) = kchange;

        let key_bytes = to_bytes(&key);
        let mut record = BaseRecord::to(&self.topic)
            .key(&key_bytes)
            .partition(self.partition);

        let payload = match change {
            Change::Upsert(value) => {
                let payload_bytes = to_bytes(&value);
                Some(payload_bytes)
            }
            Change::Discard => None,
        };
        record.payload = payload.as_ref();

        self.producer.send(record).expect("Error writing state");
        self.producer.poll(Timeout::Never);
    }
}

/// This is a generic wrapper around [`BaseConsumer`] which adds
/// serde and reads from only a single topic and partition.
pub(crate) struct KafkaReader<K, P> {
    consumer: BaseConsumer,
    key_type: PhantomData<K>,
    payload_type: PhantomData<P>,
}

impl<K, P> KafkaReader<K, P> {
    pub(crate) fn new(brokers: &[String], topic: &str, partition: i32) -> Self {
        debug!("Creating Kafka consumer with brokers={brokers:?} topic={topic:?}");
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
}

impl<K, V> KReader<K, V> for KafkaReader<K, V>
where
    K: DeserializeOwned + Debug,
    V: DeserializeOwned + Debug,
{
    fn read(&mut self) -> Option<KChange<K, V>> {
        let msg_result = self.consumer.poll(Timeout::Never);
        match msg_result {
            Some(Ok(msg)) => {
                let key = msg
                    .key()
                    .map(from_bytes)
                    .expect("Kafka recovery message did not have key");
                let change = if let Some(value) = msg.payload().map(from_bytes) {
                    Change::Upsert(value)
                } else {
                    Change::Discard
                };
                let kchange = KChange(key, change);
                trace!("Reading change {kchange:?}");
                Some(kchange)
            }
            Some(Err(KafkaError::PartitionEOF(_))) => None,
            Some(Err(err)) => panic!("Error reading from Kafka topic: {err:?}"),
            None => None,
        }
    }
}

impl<T> StateWriter<T> for KafkaWriter<StoreKey<T>, Change<StateBytes>> where T: Serialize + Debug {}
impl<T> StateReader<T> for KafkaReader<StoreKey<T>, Change<StateBytes>> where
    T: DeserializeOwned + Debug
{
}
impl<T> ProgressWriter<T> for KafkaWriter<WorkerKey, T> where T: Serialize + Debug {}
impl<T> ProgressReader<T> for KafkaReader<WorkerKey, T> where T: DeserializeOwned + Debug {}
