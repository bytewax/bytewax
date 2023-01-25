use crate::add_pymethods;
use crate::py_unwrap;
use crate::pyo3_extensions::TdPyAny;
use pyo3::{exceptions::PyTypeError, prelude::*, types::PyBytes};
use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use send_wrapper::SendWrapper;
use std::{collections::HashMap, time::Duration};

use crate::execution::{WorkerCount, WorkerIndex};

use super::{OutputBuilder, OutputConfig, OutputWriter};

/// Use [Kafka](https://kafka.apache.org) as the output.
///
/// A `capture` using KafkaOutput expects to receive data
/// structured as two-tuples of bytes (key, payload) to form a Kafka
/// record. Key may be `None`.
///
/// Args:
///
///   brokers (List[str]): List of `host:port` strings of Kafka
///       brokers.
///
///   topic (str): Topic to which producer will send records.
///
///   additional_properties (dict): Any additional configuration properties.
///       Note that consumer group settings will be ignored.
///       See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
///       for more options.
///
/// Returns:
///
///   Config object. Pass this as the `output_config` argument to the
///   `bytewax.dataflow.Dataflow.output`.
#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(brokers, topic, additional_properties)")]
#[derive(Clone)]
pub(crate) struct KafkaOutputConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic: String,
    #[pyo3(get)]
    pub(crate) additional_properties: Option<HashMap<String, String>>,
}

impl OutputBuilder for KafkaOutputConfig {
    fn build(
        &self,
        py: Python,
        _worker_index: WorkerIndex,
        _worker_count: WorkerCount,
    ) -> PyResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        let writer = py.allow_threads(|| {
            SendWrapper::new(KafkaOutput::new(
                &self.brokers,
                self.topic.to_string(),
                &self.additional_properties,
            ))
        });

        Ok(Box::new(writer.take()))
    }
}

add_pymethods!(
    KafkaOutputConfig,
    parent: OutputConfig,
    py_args: (brokers, topic, additional_properties = "None"),
    args {
        brokers: Vec<String> => Vec::new(),
        topic: String => String::new(),
        additional_properties: Option<HashMap<String, String>> => None
    }
);

/// Produce output to kafka stream
pub(crate) struct KafkaOutput {
    producer: BaseProducer,
    topic: String,
}

impl KafkaOutput {
    pub(crate) fn new(
        brokers: &[String],
        topic: String,
        additional_properties: &Option<HashMap<String, String>>,
    ) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers.join(","));

        if let Some(args) = additional_properties {
            for (key, value) in args.iter() {
                config.set(key, value);
            }
        }

        let producer: BaseProducer = config.create().expect("Producer creation error");

        Self { producer, topic }
    }
}

impl Drop for KafkaOutput {
    fn drop(&mut self) {
        tracing::debug!("Flushing producer queue");
        self.producer.flush(Duration::from_secs(5));
    }
}

impl OutputWriter<u64, TdPyAny> for KafkaOutput {
    #[tracing::instrument(name = "KafkaOutput.push", level = "trace", skip_all)]
    fn push(&mut self, _epoch: u64, key_payload: TdPyAny) {
        Python::with_gil(|py| {
            let (key, payload): (Option<TdPyAny>, TdPyAny) = py_unwrap!(
                key_payload.extract(py),
                format!(
                    "KafkaOutput requires a `(key, payload)` 2-tuple \
                    as message to producer; got `{key_payload:?}` instead"
                )
            );

            let p: &PyBytes = py_unwrap!(
                payload.extract(py),
                format!(
                    "KafkaOutput requires message payload be in bytes. Got \
                    `{payload:?}` instead"
                )
            );

            if let Some(k) = key {
                let record_key: &PyBytes = py_unwrap!(
                    k.extract(py),
                    format!(
                        "KafkaOutput requires message key be in bytes. Got \
                        `{k:?}` instead"
                    )
                );

                let record = BaseRecord::to(&self.topic)
                    .payload(p.as_bytes())
                    .key(record_key.as_bytes());

                self.producer.send(record).expect("Failed to enqueue");
            } else {
                let record: BaseRecord<[u8], [u8]> =
                    BaseRecord::to(&self.topic).payload(p.as_bytes());

                self.producer.send(record).expect("Failed to enqueue");
            };
        });

        // Poll to process all the asynchronous delivery events.
        self.producer.poll(Duration::from_millis(0));
    }
}
