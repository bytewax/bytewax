use chrono::prelude::*;
use chrono::Duration;
use pyo3::prelude::*;

use crate::add_pymethods;

use super::sliding_window::SlidingWindower;
use super::*;

/// Tumbling windows of fixed duration.
///
/// Each item will fall in exactly one window.
///
/// Window start times are inclusive, but end times are exclusive.
///
/// Args:
///   length (datetime.timedelta):
///     Length of windows.
///   align_to (datetime.datetime):
///     Align windows so this instant starts a window. This must be a
///     constant. You can use this to align all windows to hour
///     boundaries, e.g.
///
/// Returns:
///   Config object. Pass this as the `window_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct TumblingWindow {
    #[pyo3(get)]
    pub(crate) length: Duration,
    #[pyo3(get)]
    pub(crate) align_to: DateTime<Utc>,
}

impl WindowBuilder for TumblingWindow {
    fn build(&self, _py: Python) -> PyResult<Builder> {
        Ok(Box::new(SlidingWindower::builder(
            self.length,
            self.length,
            self.align_to,
        )))
    }
}

add_pymethods!(
    TumblingWindow,
    parent: WindowConfig,
    signature: (length, align_to),
    args {
        length: Duration => Duration::zero(),
        align_to: DateTime<Utc> => DateTime::<Utc>::MIN_UTC
    }
);
