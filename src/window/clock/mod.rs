use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

use crate::errors::tracked_err;
use crate::pyo3_extensions::PyConfigClass;
use crate::pyo3_extensions::TdPyAny;

use self::event_time_clock::EventClockConfig;
use self::system_clock::SystemClockConfig;

use super::Clock;

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
pub(crate) struct ClockConfig;

#[pymethods]
impl ClockConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// A type representing a function that takes an optional serialized
/// state and returns a Clock.
type Builder<V> = Box<dyn Fn(Option<TdPyAny>) -> Box<dyn Clock<V>>>;

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
            Err(tracked_err::<PyTypeError>(&format!(
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
