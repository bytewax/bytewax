use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::PyResult;

use crate::add_pymethods;

use super::*;

/// Use the current system time as the timestamp for each item.
///
/// The watermark is also the current system time.
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
    fn build(&self, _py: Python) -> PyResult<Builder<V>> {
        Ok(Box::new(move |_resume_snapshot| {
            Box::new(SystemClock { eof: false })
        }))
    }
}

add_pymethods!(
    SystemClockConfig,
    parent: ClockConfig,
    signature: (),
    args {}
);

/// Use the current system time.
pub(crate) struct SystemClock {
    eof: bool,
}

impl<V> Clock<V> for SystemClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> DateTime<Utc> {
        if let Poll::Ready(None) = next_value {
            self.eof = true;
        }

        if self.eof {
            DateTime::<Utc>::MAX_UTC
        } else {
            Utc::now()
        }
    }

    fn time_for(&mut self, _item: &V) -> DateTime<Utc> {
        Utc::now()
    }

    fn snapshot(&self) -> StateBytes {
        // Do not snapshot and restore `eof` so we support
        // continuation.
        StateBytes::ser::<()>(&())
    }
}
