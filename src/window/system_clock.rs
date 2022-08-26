use std::task::Poll;

use chrono::NaiveDateTime;
use pyo3::prelude::*;
use pyo3::{PyResult, exceptions::PyValueError};

use crate::recovery::StateBytes;

use super::{Clock, ClockConfig};

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
pub(crate) struct SystemClockConfig {}

#[pymethods]
impl SystemClockConfig {
    #[new]
    #[args()]
    fn new() -> (Self, ClockConfig) {
        (Self {}, ClockConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("SystemClockConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("SystemClockConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for SystemClockConfig: {state:?}"
            )))
        }
    }
}

/// Use the current system time.
pub(crate) struct SystemClock {}

impl SystemClock {
    pub(crate) fn builder<V>() -> impl Fn(Option<StateBytes>) -> Box<dyn Clock<V>> {
        move |_resume_state_bytes| Box::new(Self {})
    }
}

impl<V> Clock<V> for SystemClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => chrono::offset::Local::now().naive_local(),
        }
    }

    fn time_for(&mut self, item: &V) -> NaiveDateTime {
        let next_value = Poll::Ready(Some(item));
        self.watermark(&next_value)
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&())
    }
}
