use std::collections::HashMap;
use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::PyResult;

use super::*;

/// Use the system time inside the windowing operator to determine
/// times.
///
/// If the dataflow has no more input, all windows are closed.
///
/// Returns:
///
///   Config object. Pass this as the `clock_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[derive(Clone)]
pub(crate) struct SystemClockConfig {}

impl<V> ClockBuilder<V> for SystemClockConfig {
    fn build(&self, _py: Python) -> StringResult<Builder<V>> {
        Ok(Box::new(move |_resume_snapshot| {
            Box::new(SystemClock::new())
        }))
    }
}

#[pymethods]
impl SystemClockConfig {
    #[new]
    #[args()]
    fn new() -> (Self, ClockConfig) {
        (Self {}, ClockConfig {})
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "SystemClockConfig".into_py(py))]))
    }

    /// Unpickle from a PyDict of arguments.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

/// Use the current system time.
pub(crate) struct SystemClock {}

impl SystemClock {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl<V> Clock<V> for SystemClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> DateTime<Utc> {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => DateTime::<Utc>::MAX_UTC,
            _ => Utc::now(),
        }
    }

    fn time_for(&mut self, _item: &V) -> DateTime<Utc> {
        Utc::now()
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<()>(&())
    }
}
