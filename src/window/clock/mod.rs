use std::collections::HashMap;

use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::pyo3_extensions::{PyConfigClass, TdPyAny};

use self::{event_time_clock::EventClockConfig, system_clock::SystemClockConfig};

use super::{Clock, StateBytes};

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

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "ClockConfig".into_py(py))]))
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

/// A type representing a function that takes an optional serialized
/// state and returns a Clock.
type Builder<V> = Box<dyn Fn(Option<StateBytes>) -> Box<dyn Clock<V>>>;

/// The `builder` function consumes a ClockConfig to build a Clock.
pub(crate) trait ClockBuilder<V> {
    fn build(&self, py: Python) -> PyResult<Builder<V>>;
}

impl PyConfigClass<Box<dyn ClockBuilder<TdPyAny>>> for Py<ClockConfig> {
    fn downcast(&self, py: Python) -> PyResult<Box<dyn ClockBuilder<TdPyAny>>> {
        if let Ok(conf) = self.extract::<SystemClockConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<EventClockConfig>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(PyTypeError::new_err(format!(
                "Unknown clock_config type: {pytype}"
            )))
        }
    }
}

impl ClockBuilder<TdPyAny> for Py<ClockConfig> {
    fn build(&self, py: Python) -> PyResult<Builder<TdPyAny>> {
        self.downcast(py)?.build(py)
    }
}
