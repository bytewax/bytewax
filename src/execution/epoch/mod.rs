pub(crate) mod periodic_epoch;
pub(crate) mod testing_epoch;

use pyo3::{exceptions::PyValueError, pyclass, pymethods, Py, PyAny, PyCell, PyResult, Python};
use timely::dataflow::{ProbeHandle, Scope, Stream};

use crate::{
    common::StringResult,
    inputs::InputReader,
    operators::stateful_unary::{FlowChangeStream, StepId},
    pyo3_extensions::{PyConfigClass, TdPyAny},
    recovery::model::state::StateKey,
};

use self::{periodic_epoch::PeriodicEpochConfig, testing_epoch::TestingEpochConfig};

/// Base class for an epoch config.
///
/// These define how epochs are assigned on source input data. You
/// should only need to set this if you are testing the recovery
/// system or are doing deep exactly-once integration work. Changing
/// this does not change the semantics of any of the operators.
///
/// Use a specific subclass of this for the epoch definition you need.
#[pyclass(module = "bytewax.execution", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct EpochConfig;

impl EpochConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, EpochConfig {}).unwrap().into()
    }
}

#[pymethods]
impl EpochConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("EpochConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("EpochConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for EpochConfig: {state:?}"
            )))
        }
    }
}

pub(crate) trait EpochBuilder<S: Scope<Timestamp = u64>> {
    fn build(
        &self,
        py: Python,
        scope: &S,
        step_id: StepId,
        key: StateKey,
        reader: Box<dyn InputReader<TdPyAny>>,
        start_at: S::Timestamp,
        probe: &ProbeHandle<S::Timestamp>,
    ) -> StringResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)>;
}

impl<S> EpochBuilder<S> for Py<EpochConfig>
where
    S: Scope<Timestamp = u64>,
{
    fn build(
        &self,
        py: Python,
        scope: &S,
        step_id: StepId,
        key: StateKey,
        reader: Box<dyn InputReader<TdPyAny>>,
        start_at: S::Timestamp,
        probe: &ProbeHandle<S::Timestamp>,
    ) -> StringResult<(Stream<S, TdPyAny>, FlowChangeStream<S>)> {
        self.downcast(py)?
            .build(py, scope, step_id, key, reader, start_at, probe)
    }
}

impl<S> PyConfigClass<Box<dyn EpochBuilder<S>>> for Py<EpochConfig>
where
    S: Scope<Timestamp = u64>,
{
    fn downcast(&self, py: Python) -> StringResult<Box<dyn EpochBuilder<S>>> {
        if let Ok(config) = self.extract::<TestingEpochConfig>(py) {
            Ok(Box::new(config))
        } else if let Ok(config) = self.extract::<PeriodicEpochConfig>(py) {
            Ok(Box::new(config))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(format!("Unknown epoch_config type: {pytype}"))
        }
    }
}

/// Default to 10 second periodic epochs.
pub(crate) fn default_epoch_config() -> Py<EpochConfig> {
    Python::with_gil(|py| {
        PyCell::new(py, PeriodicEpochConfig::new(chrono::Duration::seconds(10)))
            .unwrap()
            .extract()
            .unwrap()
    })
}
