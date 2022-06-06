use log::{info};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;
use std::time::Duration;
use tokio::runtime::Runtime;

pub enum TimelyAction {
    AdvanceTo(u64),
    Emit(String),
}
pub(crate) struct KafkaConsumer {
    rt: Runtime,
    consumer: BaseConsumer<CustomContext>,
    // eventually resume_epoch...
    current_epoch: u64,
    desired_batch_size: u64,
    current_batch_size: u64,
}

impl KafkaConsumer {
    pub(crate) fn new(brokers: &str, group_id: &str, topics: &str, batch_size: u64) -> Self {
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
            current_epoch: 0,
            desired_batch_size: batch_size,
            current_batch_size: 0,
        }
    }

    pub fn next(&mut self) -> TimelyAction {
        if self.current_batch_size == self.desired_batch_size {
            dbg!("Batch complete, incrementing epoch");
            self.current_batch_size = 0;
            self.current_epoch += 1;
            return TimelyAction::AdvanceTo(self.current_epoch);
        } else {
            // TODO async loop to populate batch buffer
            self.rt.block_on(async {
                // I have no sense of an appropriate timeout. I suppose users provide?
                match self.consumer.poll(Duration::from_millis(1000)) {
                    None => {
                        dbg!("No messages available, incrementing epoch");
                        self.current_batch_size = 0;
                        self.current_epoch += 1;
                        TimelyAction::AdvanceTo(self.current_epoch)
                    }
                    Some(r) => match r {
                        Err(e) => panic!("Kafka error! {}", e),
                        Ok(s) => match s.payload_view::<str>() {
                            Some(Ok(r)) => {
                                self.current_batch_size += 1;
                                TimelyAction::Emit(r.to_owned())
                            }
                            Some(Err(e)) => {
                                panic!("Could not deserialize Kafka msg with error {}", e)
                            }
                            None => {
                                panic!("Payload was empty");
                            }
                        },
                    },
                }
            })
        }
    }
}

//  // A context can be used to change the behavior of producers and consumers by adding callbacks
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
