use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use std::time::Duration;
use tokio::runtime::Runtime;

pub(crate) struct KafkaConsumer {
    rt: Runtime,
    consumer: BaseConsumer<CustomContext>,
    pub empty: bool,
}

impl KafkaConsumer {
    pub(crate) fn new(brokers: &str, group_id: &str, topics: &str) -> Self {
        let context = CustomContext;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let consumer: BaseConsumer<CustomContext> = rt.block_on(async {
            ClientConfig::new()
                .set("group.id", group_id)
                .set("bootstrap.servers", brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                // TODO: we don't really want false here, it's for demo purposes
                .set("enable.auto.commit", "false")
                //.set("statistics.interval.ms", "30000")
                .set("auto.offset.reset", "earliest")
                .set_log_level(RDKafkaLogLevel::Debug)
                .create_with_context(context)
                .expect("Consumer creation failed")
        });

        let t = vec![topics];
        consumer
            .subscribe(&t)
            .expect("Can't subscribe to specified topics");

        Self {
            rt,
            consumer,
            empty: false,
        }
    }

    pub fn next(&mut self) -> Option<String> {
        self.rt.block_on(async {
            match self.consumer.poll(Duration::from_millis(5)) {
                None => None,
                Some(r) => match r {
                    Err(e) => panic!("Kafka error! {}", e),
                    Ok(s) => match s.payload_view::<str>() {
                        Some(Ok(r)) => Some(r.to_owned()),
                        Some(Err(e)) => panic!("Could not deserialize Kafka msg with error {}", e),
                        None => {
                            warn!("Payload was empty");
                            None
                        }
                    },
                },
            }
        })
    }
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
