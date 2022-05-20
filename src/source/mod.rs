
use log::{info, warn};

use pyo3::prelude::*;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::runtime::Runtime;

#[pyclass]
#[pyo3(text_signature = "()")]
pub(crate) struct KafkaConsumer {
    rt: Runtime,
    consumer: StreamConsumer<CustomContext>
}

#[pymethods]
impl KafkaConsumer {
    #[new]
    fn new(brokers: &str, group_id: &str) -> Self {
        let context = CustomContext;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let consumer: StreamConsumer<CustomContext> =rt.block_on(async {
            ClientConfig::new()
                .set("group.id", group_id)
                .set("bootstrap.servers", brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "true")
                //.set("statistics.interval.ms", "30000")
                .set("auto.offset.reset", "earliest")
                .set_log_level(RDKafkaLogLevel::Debug)
                .create_with_context(context)
                .expect("Consumer creation failed")
        });
        
        Self {
            rt,
            consumer,
        }
    }

    fn __next__(&self) -> Option<String> {
        self.rt.block_on(async {
            match self.consumer.recv().await {
                Err(e) => panic!("Kafka error: {}", e),
                Ok(m) => {
                    let payload = match m.payload_view::<str>() {
                        None => "",
                        Some(Ok(s)) => s,
                        Some(Err(e)) => {
                            warn!("Error while deserializing message payload: {:?}", e);
                            ""
                        }
                    };
                    info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                            m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                    // TODO: how to return none here
                    Some(payload.to_owned())
                }
            }
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
}

#[pyfunction(brokers, group_id, topics)]
#[pyo3(text_signature = "(brokers, group_id, topics)")]
pub(crate) fn consume_from_kafka(brokers: &str, group_id: &str, topics: Vec<&str>) -> KafkaConsumer {
    let kconsumer = KafkaConsumer::new(brokers, group_id);

    kconsumer
        .consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");
    
        return kconsumer
}


// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// #[pyfunction(brokers, group_id, topics)]
// #[pyo3(text_signature = "(brokers, group_id, topics)")]
// pub(crate) fn consume_from_kafka(brokers: &str, group_id: &str, topics: Vec<&str>) {
//     let context = CustomContext;

//     let consumer: StreamConsumer<CustomContext> = ClientConfig::new()
//         .set("group.id", group_id)
//         .set("bootstrap.servers", brokers)
//         .set("enable.partition.eof", "false")
//         .set("session.timeout.ms", "6000")
//         .set("enable.auto.commit", "true")
//         //.set("statistics.interval.ms", "30000")
//         .set("auto.offset.reset", "earliest")
//         .set_log_level(RDKafkaLogLevel::Debug)
//         .create_with_context(context)
//         .expect("Consumer creation failed");



//     consumer
//         .subscribe(&topics)
//         .expect("Can't subscribe to specified topics");
    
//     let rt = tokio::runtime::Builder::new_current_thread()
//         .enable_all()
//         .build()
//         .unwrap();
    
//     loop {
//         rt.block_on(async {
//             match consumer.recv().await {
//                 Err(e) => warn!("Kafka error: {}", e),
//                 Ok(m) => {
//                     let payload = match m.payload_view::<str>() {
//                         None => "",
//                         Some(Ok(s)) => s,
//                         Some(Err(e)) => {
//                             warn!("Error while deserializing message payload: {:?}", e);
//                             ""
//                         }
//                     };
//                     info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
//                             m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
//                 }
//             }
//         });
//     }
// }


pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(consume_from_kafka, m)?)?;
    Ok(())
}
