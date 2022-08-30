use crate::pyo3_extensions::TdPyAny;
use pyo3::{exceptions::PyValueError, prelude::*};
use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use std::time::Duration;

use super::{OutputConfig, OutputWriter};
/// Use [Kafka](https://kafka.apache.org) as the output.
///
/// A `capture` using KafkaOutput expects to receive data
/// structured as two-tuples of (key, payload) to form a Kafka
/// record.
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic (str): Topic to which producer will send records.
///
/// Returns:
///
///   Config object. Pass this as the `output_config` argument to the
///   `bytewax.dataflow.Dataflow.output`.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(brokers, topic)")]
pub(crate) struct KafkaOutputConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic: String,
}

#[pymethods]
impl KafkaOutputConfig {
    #[new]
    #[args(brokers, topic)]
    fn new(brokers: Vec<String>, topic: String) -> (Self, OutputConfig) {
        (Self { brokers, topic }, OutputConfig {})
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String) {
        (
            "KafkaOutputConfig",
            self.brokers.clone(),
            self.topic.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaOutputConfig", brokers, topic)) = state.extract() {
            self.brokers = brokers;
            self.topic = topic;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaOutputConfig: {state:?}"
            )))
        }
    }
}

/// Produce output to kafka stream
pub(crate) struct KafkaOutput {
    producer: BaseProducer,
    topic: String,
}

impl KafkaOutput {
    pub(crate) fn new(brokers: &[String], topic: String) -> Self {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers.join(","))
            .create()
            .expect("Producer creation error");

        Self { producer, topic }
    }
}

impl OutputWriter<u64, TdPyAny> for KafkaOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        let item_string = item.to_string();
        self.producer
            .send(
                BaseRecord::to(&self.topic)
                    .payload(&item_string)
                    // TODO configure key
                    .key("and this is a key"),
            )
            .expect("Failed to enqueue");

        // Poll to process all the asynchronous delivery events.
        self.producer.poll(Duration::from_millis(0));

        // And/or flush the producer before dropping it.
        self.producer.flush(Duration::from_secs(1));
    }
}
