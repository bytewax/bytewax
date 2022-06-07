use crate::pyo3_extensions::{TdPyAny, TdPyIterator};
use crate::source::{KafkaConsumer, TimelyAction};

use pyo3::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use timely::dataflow::InputHandle;

/// Advance to the supplied epoch.
///
/// When providing input to a Dataflow, work cannot complete until
/// there is no more data for a given epoch.
///
/// AdvanceTo is the signal to a Dataflow that the frontier has moved
/// beyond the current epoch, and that items with an epoch less than
/// the epoch in AdvanceTo can be worked to completion.
///
/// Using AdvanceTo and Emit is only necessary when using `spawn_cluster`
/// and `cluster_main()` as `run()` and `run_cluster()` will yield AdvanceTo
/// and Emit for you.
///
/// See also: `inputs.yield_epochs()`
///
/// >>> def input_builder(worker_index, worker_count):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
#[pyclass(module = "bytewax")]
#[pyo3(text_signature = "(epoch)")]
pub(crate) struct AdvanceTo {
    #[pyo3(get)]
    epoch: u64,
}

#[pymethods]
impl AdvanceTo {
    #[new]
    fn new(epoch: u64) -> Self {
        Self { epoch }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Lt => Ok(self.epoch < other.epoch),
            CompareOp::Le => Ok(self.epoch <= other.epoch),
            CompareOp::Eq => Ok(self.epoch == other.epoch),
            CompareOp::Ne => Ok(self.epoch != other.epoch),
            CompareOp::Gt => Ok(self.epoch > other.epoch),
            CompareOp::Ge => Ok(self.epoch >= other.epoch),
        }
    }
}

/// Emit the supplied item into the dataflow at the current epoch
///
/// Emit is how we introduce input into a dataflow:
///
/// >>> def input_builder(worker_index, worker_count):
/// ...     for i in range(10):
/// ...         yield AdvanceTo(i) # Advances the epoch to i
/// ...         yield Emit(i) # Adds the input i at epoch i
#[pyclass(module = "bytewax")]
#[pyo3(text_signature = "(item)")]
pub(crate) struct Emit {
    #[pyo3(get)]
    item: TdPyAny,
}

#[pymethods]
impl Emit {
    #[new]
    fn new(item: Py<PyAny>) -> Self {
        Self { item: item.into() }
    }
}

#[pyclass(module = "bytewax.input", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct InputConfig;

#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(brokers, group_id, topics)")]
pub(crate) struct KafkaInputConfig {
    #[pyo3(get)]
    pub brokers: String,
    #[pyo3(get)]
    pub group_id: String,
    #[pyo3(get)]
    pub topics: String,
    #[pyo3(get)]
    pub batch_size: u64,
}

#[pymethods]
impl KafkaInputConfig {
    #[new]
    #[args(brokers, group_id, topics, batch_size = 1)]
    fn new(
        brokers: String,
        group_id: String,
        topics: String,
        batch_size: u64,
    ) -> (Self, InputConfig) {
        (
            Self {
                brokers,
                group_id,
                topics,
                batch_size,
            },
            InputConfig {},
        )
    }

    fn __getstate__(&self) -> (&str, String, String, String, u64) {
        (
            "KafkaInputConfig",
            self.brokers.clone(),
            self.group_id.clone(),
            self.topics.clone(),
            self.batch_size,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str, &str, &str, u64) {
        let s = "UNINIT_PICKLED_STRING";
        (s, s, s, 0)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("KafkaInputConfig", brokers, group_id, topics, batch_size)) = state.extract() {
            self.brokers = brokers;
            self.group_id = group_id;
            self.topics = topics;
            self.batch_size = batch_size;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for KafkaInputConfig: {state:?}"
            )))
        }
    }
}

#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(gen_func)")]
pub(crate) struct ManualInputConfig {
    gen_func: Py<PyAny>,
}

#[pymethods]
impl ManualInputConfig {
    #[new]
    #[args(gen_func)]
    fn new(gen_func: Py<PyAny>) -> (Self, InputConfig) {
        (Self { gen_func }, InputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self, py: Python) -> (&str, Py<PyAny>) {
        ("ManualInputConfig", self.gen_func.clone_ref(py))
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (&str,) {
        (&"",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualInputConfig", gen_func)) = state.extract() {
            self.gen_func = gen_func;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualInputConfig: {state:?}"
            )))
        }
    }
}

// Take a single data element or timestamp and feed it into dataflow
/// This will be called in the worker's "main" loop to feed data in.
pub trait Pump {
    fn pump(&mut self);
    fn input_time(&mut self) -> &u64;
    fn input_remains(&mut self) -> bool;
}

/// Encapsulates the process of pulling data out of a Kafka
/// stream and feeding it into Timely.
pub(crate) struct KafkaPump {
    kafka_consumer: KafkaConsumer,
    push_to_timely: InputHandle<u64, TdPyAny>,
    empty: bool,
}

impl KafkaPump {
    pub(crate) fn new(
        config: PyRef<KafkaInputConfig>,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        let kafka_consumer = KafkaConsumer::new(config);
        Self {
            kafka_consumer,
            push_to_timely,
            empty: false,
        }
    }
}

impl Pump for KafkaPump {
    fn pump(&mut self) {
        match self.kafka_consumer.next() {
            TimelyAction::AdvanceTo(epoch) => self.push_to_timely.advance_to(epoch),
            TimelyAction::Emit(item) => Python::with_gil(|py| {
                let py_any_string = TdPyAny::from(PyString::new(py, &item));
                self.push_to_timely.send(py_any_string);
            }),
        }
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.empty
    }
}

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
pub(crate) struct PyPump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl PyPump {
    pub(crate) fn new(
        py: Python,
        worker_index: usize,
        worker_count: usize,
        resume_epoch: u64,
        config: PyRef<ManualInputConfig>,
        push_to_timely: InputHandle<u64, TdPyAny>,
    ) -> Self {
        let worker_input: TdPyIterator = config
            .gen_func
            .call1(py, (worker_index, worker_count, resume_epoch))
            .unwrap()
            .extract(py)
            .unwrap();
        Self {
            pull_from_pyiter: worker_input,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }
}

impl Pump for PyPump {
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(input_or_action) = pull_from_pyiter.next() {
                match input_or_action {
                    Ok(item) => {
                        if let Ok(send) = item.downcast::<PyCell<Emit>>() {
                            self.push_to_timely.send(send.borrow().item.clone());
                        } else if let Ok(advance_to) = item.downcast::<PyCell<AdvanceTo>>() {
                            self.push_to_timely.advance_to(advance_to.borrow().epoch);
                        } else {
                            panic!("{}", format!("Input must be an instance of either `AdvanceTo` or `Emit`. Got: {item:?}. See https://docs.bytewax.io/apidocs#bytewax.AdvanceTo for more information."))
                        }
                    }
                    Err(err) => {
                        std::panic::panic_any(err);
                    }
                }
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_time(&mut self) -> &u64 {
        self.push_to_timely.time()
    }

    fn input_remains(&mut self) -> bool {
        !self.pyiter_is_empty
    }
}

pub(crate) fn pump_from_config(
    py: Python,
    config: Py<InputConfig>,
    input_handle: InputHandle<u64, TdPyAny>,
    worker_index: usize,
    worker_count: usize,
    resume_epoch: u64,
) -> Box<dyn Pump> {
    let input_config = config.as_ref(py);
    if let Ok(kafka_config) = input_config.downcast::<PyCell<KafkaInputConfig>>() {
        let kafka_config = kafka_config.borrow();
        Box::new(KafkaPump::new(kafka_config, input_handle))
    } else if let Ok(manual_config) = input_config.downcast::<PyCell<ManualInputConfig>>() {
        let manual_config = manual_config.borrow();
        Box::new(PyPump::new(
            py,
            worker_index,
            worker_count,
            resume_epoch,
            manual_config,
            input_handle,
        ))
    } else {
        let pytype = input_config.get_type();
        panic!("Unknown input_config type: {pytype}")
    }
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Emit>()?;
    m.add_class::<AdvanceTo>()?;
    m.add_class::<InputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    Ok(())
}
