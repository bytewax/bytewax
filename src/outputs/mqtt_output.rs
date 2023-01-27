extern crate paho_mqtt as mqtt;

use std::collections::HashMap;

use mqtt::{Client, ConnectOptions, SyncTopic};
use pyo3::{pyclass, pymethods, types::PyDict, IntoPy, Py, PyAny, PyResult, Python};
use send_wrapper::SendWrapper;

use crate::{
    common::{pickle_extract, StringResult},
    execution::WorkerIndex,
    outputs::OutputConfig,
    pyo3_extensions::TdPyAny,
};

use super::{OutputBuilder, OutputWriter};

#[pyclass(module = "bytewax.outputs", extends = OutputConfig)]
#[pyo3(text_signature = "(host, topics)")]
#[derive(Clone)]
pub(crate) struct MqttOutputConfig {
    #[pyo3(get)]
    pub(crate) host: String,
    #[pyo3(get)]
    pub(crate) topic: String,
}

impl OutputBuilder for MqttOutputConfig {
    fn build(
        &self,
        py: Python,
        _worker_index: WorkerIndex,
        _worker_count: usize,
    ) -> StringResult<Box<dyn OutputWriter<u64, TdPyAny>>> {
        let writer =
            py.allow_threads(|| SendWrapper::new(MqttOutput::new(&self.host, &self.topic)));
        Ok(Box::new(writer.take()))
    }
}

#[pymethods]
impl MqttOutputConfig {
    #[new]
    #[args(host, topic)]
    fn new(host: String, topic: String) -> (Self, OutputConfig) {
        (Self { host, topic }, OutputConfig {})
    }

    // Usual boilerplate here
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "MqttOutputConfig".into_py(py)),
                ("host", self.host.clone().into_py(py)),
                ("topic", self.topic.clone().into_py(py)),
            ])
        })
    }

    fn __getnewargs__(&self) -> (&str, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (s, s)
    }

    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.topic = pickle_extract(dict, "topic")?;
        self.host = pickle_extract(dict, "host")?;
        Ok(())
    }
}

pub(crate) struct MqttOutput {
    client: Client,
    topic: String,
}

impl MqttOutput {
    pub fn new(host: &str, topic: &str) -> Self {
        // Create a client & define connect options
        let client = Client::new(host).unwrap();
        let conn_opts = ConnectOptions::new();
        // Connect
        client.connect(conn_opts).unwrap();
        Self {
            client,
            topic: topic.to_string(),
        }
    }
}

impl OutputWriter<u64, TdPyAny> for MqttOutput {
    fn push(&mut self, _epoch: u64, item: TdPyAny) {
        // TODO: Avoid instantiating a new topic at every push.
        // I did this because the `SyncTopic` struct holds a reference to the client itself,
        // and if we want to store both in our structure we make the borrow checker unhappy.
        let topic = SyncTopic::new(&self.client, &self.topic, 1);
        topic.publish(item.to_string()).unwrap();
    }
}
