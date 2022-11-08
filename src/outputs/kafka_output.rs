use crate::{py_unwrap, pickle_extract};
use crate::pyo3_extensions::TdPyAny;
use pyo3::types::PyDict;
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::PyBytes,
};
use rdkafka::{
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig,
};
use send_wrapper::SendWrapper;
use std::{collections::HashMap, time::Duration};

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
        _worker_index: crate::execution::WorkerIndex,
        _worker_count: usize,
    ) -> crate::common::StringResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
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

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "KafkaOutputConfig".into_py(py)),
                ("brokers", self.brokers.clone().into_py(py)),
                ("topic", self.topic.clone().into_py(py)),
                ("additional_properties", self.additional_properties.clone().into_py(py))
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (Vec<String>, &str, Option<HashMap<String, String>>) {
        let s = "UNINIT_PICKLED_STRING";
        (Vec::new(), s, None)
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        pickle_extract!(self, dict, brokers);
        pickle_extract!(self, dict, topic);
        pickle_extract!(self, dict, additional_properties);
        Ok(())
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
