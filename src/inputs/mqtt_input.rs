extern crate paho_mqtt as mqtt;

use std::{collections::HashMap, task::Poll, time::Duration};

use mqtt::{Message, Receiver};
use pyo3::{
    pyclass, pymethods,
    types::{PyBytes, PyDict},
    IntoPy, Py, PyAny, PyResult, Python,
};
use send_wrapper::SendWrapper;

use crate::{
    common::{pickle_extract, StringResult},
    execution::WorkerIndex,
    inputs::InputConfig,
    pyo3_extensions::TdPyAny,
    window::StateBytes,
};

use super::{InputBuilder, InputReader};

#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[derive(Clone)]
pub(crate) struct MqttInputConfig {
    pub(crate) host: String,
    pub(crate) topics: Vec<String>,
    pub(crate) qos: Vec<i32>,
    pub(crate) client_id: String,
}

impl InputBuilder for MqttInputConfig {
    fn build(
        &self,
        py: pyo3::Python,
        _worker_index: WorkerIndex,
        _worker_count: usize,
        _resume_snapshot: Option<StateBytes>,
    ) -> StringResult<Box<dyn InputReader<TdPyAny>>> {
        let reader = py.allow_threads(|| {
            SendWrapper::new(MqttInput::new(
                &self.host,
                &self.client_id,
                &self.topics,
                &self.qos,
            ))
        });
        Ok(Box::new(reader.take()))
    }
}

#[pymethods]
impl MqttInputConfig {
    #[new]
    #[args(host, topics, qos, client_id)]
    fn new(
        host: String,
        topics: Vec<String>,
        qos: Vec<i32>,
        client_id: String,
    ) -> (Self, InputConfig) {
        (
            Self {
                host,
                topics,
                qos,
                client_id,
            },
            InputConfig {},
        )
    }
    /// Pickle as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| {
            HashMap::from([
                ("type", "MqttInputConfig".into_py(py)),
                ("host", self.host.clone().into_py(py)),
                ("topics", self.topics.clone().into_py(py)),
                ("qos", self.qos.clone().into_py(py)),
                ("client_id", self.client_id.clone().into_py(py)),
            ])
        })
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str, Vec<String>, Vec<i32>, &str) {
        let s = "UNINIT_PICKLED_STRING";
        (s, Vec::new(), Vec::new(), s)
    }

    /// Unpickle from a PyDict of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        let dict: &PyDict = state.downcast()?;
        self.host = pickle_extract(dict, "host")?;
        self.topics = pickle_extract(dict, "topics")?;
        self.qos = pickle_extract(dict, "qos")?;
        self.client_id = pickle_extract(dict, "client_id")?;
        Ok(())
    }
}

pub(crate) struct MqttInput {
    client: mqtt::Client,
    rx: Receiver<Option<Message>>,
    topics: Vec<String>,
    qos: Vec<i32>,
}

impl MqttInput {
    pub(crate) fn new(host: &String, client_id: &String, topics: &[String], qos: &[i32]) -> Self {
        // Use an ID for a persistent session.
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(host)
            .client_id(client_id)
            .finalize();

        // Create a client.
        let client = mqtt::Client::new(create_opts).unwrap();

        let rx = client.start_consuming();

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(false)
            .finalize();

        // Connect and wait for it to complete or fail.
        client.connect(conn_opts).unwrap();

        client.subscribe_many(topics, qos).unwrap();
        Self {
            client,
            rx,
            topics: topics.to_vec(),
            qos: qos.to_vec(),
        }
    }
}

impl InputReader<TdPyAny> for MqttInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        match self.rx.try_recv() {
            Ok(Some(msg)) => {
                let payload = msg.payload();
                let item: TdPyAny = Python::with_gil(|py| {
                    let payload: Py<PyAny> = PyBytes::new(py, payload).into();
                    payload.into()
                });
                Poll::Ready(Some(item))
            }
            Ok(None) => Poll::Pending,
            Err(err) => {
                if err.is_empty() {
                    Poll::Pending
                } else {
                    // Else, err.is_disconnected() is true.
                    // Try to reconnect and resubscribe before bailing...
                    if self.client.reconnect().is_ok()
                        && self.client.subscribe_many(&self.topics, &self.qos).is_ok()
                    {
                        Poll::Pending
                    } else {
                        Poll::Ready(None)
                    }
                }
            }
        }
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<()>(&())
    }
}
