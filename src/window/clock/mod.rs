use pyo3::{pyclass, PyCell, Python, Py, pymethods, exceptions::PyValueError, PyAny, PyResult};

use crate::{common::{StringResult, ParentClass}, pyo3_extensions::TdPyAny};

use self::{system_clock::SystemClockConfig, event_time_clock::EventClockConfig};

use super::{StateBytes, Clock};

pub(crate) mod event_time_clock;
pub(crate) mod system_clock;

/// Base class for a clock config.
///
/// This describes how a windowing operator should determine the
/// current time and the time for each element.
///
/// Use a specific subclass of this that matches the time definition
/// you'd like to use.
#[pyclass(module = "bytewax.window", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct ClockConfig;

impl ClockConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, ClockConfig {}).unwrap().into()
    }
}

#[pymethods]
impl ClockConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("ClockConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ClockConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ClockConfig: {state:?}"
            )))
        }
    }
}

/// A type representing a function that takes an optional serialized
/// state and returns a Clock.
type Builder<V> = Box<dyn Fn(Option<StateBytes>) -> Box<dyn Clock<V>>>;

/// The `builder` function consumes a ClockConfig to build a Clock.
pub(crate) trait ClockBuilder<V> {
    fn build(&self, py: Python) -> StringResult<Builder<V>>;
}

impl ParentClass for Py<ClockConfig> {
    type Children = Box<dyn ClockBuilder<TdPyAny>>;

    fn get_subclass(&self, py: Python) -> StringResult<Self::Children> {
        if let Ok(conf) = self.extract::<SystemClockConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<EventClockConfig>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(format!("Unknown clock_config type: {pytype}",))
        }
    }
}

impl ClockBuilder<TdPyAny> for Py<ClockConfig> {
    fn build(&self, py: Python) -> StringResult<Builder<TdPyAny>> {
        self.get_subclass(py)?.build(py)
    }
}
