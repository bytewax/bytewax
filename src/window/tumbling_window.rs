use chrono::prelude::*;
use chrono::TimeDelta;
use pyo3::prelude::*;

use super::sliding_window::SlidingWindower;
use super::*;

/// Tumbling windows of fixed duration.
///
/// Each item will fall in exactly one window.
///
/// Window start times are inclusive, but end times are exclusive.
///
/// :arg length: Length of windows.
///
/// :type length: datetime.timedelta
///
/// :arg align_to: Align windows so this instant starts a window. This
///     must be a constant. You can use this to align all windows to
///     hour boundaries, e.g.
///
/// :type align_to: datetime.timedelta
///
/// :returns: Config object. Pass this as the `window_config`
///     parameter to your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct TumblingWindow {
    #[pyo3(get)]
    pub(crate) length: TimeDelta,
    #[pyo3(get)]
    pub(crate) align_to: DateTime<Utc>,
}

#[pymethods]
impl TumblingWindow {
    #[new]
    fn new(length: TimeDelta, align_to: DateTime<Utc>) -> (Self, WindowConfig) {
        let self_ = Self { length, align_to };
        let super_ = WindowConfig::new();
        (self_, super_)
    }
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
