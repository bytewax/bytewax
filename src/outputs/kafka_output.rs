use crate::py_unwrap;
use crate::pyo3_extensions::TdPyAny;
use log::debug;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::PyBytes,
};
use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use std::{collections::HashMap, time::Duration};

use super::{OutputConfig, OutputWriter};
/// Use [Kafka](https://kafka.apache.org) as the output.
///
/// A `capture` using KafkaOutput expects to receive data
/// structured as two-tuples of (key, payload) to form a Kafka
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
pub(crate) struct KafkaOutputConfig {
    #[pyo3(get)]
    pub(crate) brokers: Vec<String>,
    #[pyo3(get)]
    pub(crate) topic: String,
    #[pyo3(get)]
    pub(crate) additional_properties: Option<HashMap<String, String>>,
}

#[pymethods]
impl KafkaOutputConfig {
    #[new]
    #[args(brokers, topic, additional_properties = "None")]
    fn new(
        brokers: Vec<String>,
        topic: String,
        additional_properties: Option<HashMap<String, String>>,
    ) -> (Self, OutputConfig) {
        (
            Self {
                brokers,
                topic,
                additional_properties,
            },
            OutputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, Vec<String>, String, Option<HashMap<String, String>>) {
        (
            "KafkaOutputConfig",
            self.brokers.clone(),
            self.topic.clone(),
            self.additional_properties.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, Option<HashMap<String, String>>) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaOutputConfig", brokers, topic, additional_properties)) = state.extract() {
            self.brokers = brokers;
            self.topic = topic;
            self.additional_properties = additional_properties;
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
        debug!("Flushing producer queue");
        self.producer.flush(Duration::from_secs(5));
    }
}

impl OutputWriter<u64, TdPyAny> for KafkaOutput {
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
