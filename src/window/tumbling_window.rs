use chrono::prelude::*;
use chrono::Duration;
use pyo3::prelude::*;

use super::sliding_window::SlidingWindower;
use super::*;

/// Tumbling windows of fixed duration.
///
/// Each item will fall in exactly one window.
///
/// Window start times are inclusive, but end times are exclusive.
///
/// Args:
///
///   length (datetime.timedelta): Length of windows.
///
///   align_to (datetime.datetime): Align windows so this instant
///     starts a window. This must be a constant. You can use this to
///     align all windows to hour boundaries, e.g.
///
/// Returns:
///
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

#[pymethods]
impl TumblingWindow {
    #[new]
    fn new(length: Duration, align_to: DateTime<Utc>) -> (Self, WindowConfig) {
        let self_ = Self { length, align_to };
        let super_ = WindowConfig::new();
        (self_, super_)
    }

    fn __json__<'py>(&self, py: Python<'py>) -> PyResult<&'py PyDict> {
        let dict = PyDict::new(py);
        dict.set_item("type", "TumblingWindow")?;
        dict.set_item("length", self.length)?;
        dict.set_item("align_to", self.align_to)?;
        Ok(dict)
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
