use chrono::{DateTime, Duration, Utc};
use pyo3::prelude::*;

use crate::add_pymethods;

use super::*;

/// Tumbling windows of fixed duration.
///
/// Args:
///
///   length (datetime.timedelta): Length of window.
///
///   start_at (datetime.datetime): Instant of the first window. You
///       can use this to align all windows to an hour,
///       e.g. Defaults to system time of dataflow start.
///
/// Returns:
///
///   Config object. Pass this as the `window_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct TumblingWindow {
    #[pyo3(get)]
    pub(crate) length: chrono::Duration,
    #[pyo3(get)]
    pub(crate) start_at: Option<DateTime<Utc>>,
}

impl WindowBuilder for TumblingWindow {
    fn build(&self, _py: Python) -> PyResult<Builder> {
        Ok(Box::new(super::sliding_window::SlidingWindower::builder(
            self.length,
            self.length,
            self.start_at.unwrap_or_else(Utc::now),
        )))
    }
}

add_pymethods!(
    TumblingWindow,
    parent: WindowConfig,
    py_args: (length, start_at = "None"),
    args {
        length: Duration => Duration::zero(),
        start_at: Option<DateTime<Utc>> => None
    }
);
