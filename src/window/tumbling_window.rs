use std::collections::HashMap;

use chrono::{Duration, NaiveDateTime};
use pyo3::{exceptions::PyValueError, prelude::*};

use crate::recovery::StateBytes;

use super::{InsertError, WindowConfig, WindowKey, Windower};

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
pub(crate) struct TumblingWindowConfig {
    #[pyo3(get)]
    pub(crate) length: pyo3_chrono::Duration,
    #[pyo3(get)]
    pub(crate) start_at: Option<pyo3_chrono::NaiveDateTime>,
}

#[pymethods]
impl TumblingWindowConfig {
    #[new]
    #[args(length, start_at = "None")]
    pub(crate) fn new(
        length: pyo3_chrono::Duration,
        start_at: Option<pyo3_chrono::NaiveDateTime>,
    ) -> (Self, WindowConfig) {
        (Self { length, start_at }, WindowConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(
        &self,
    ) -> (
        &str,
        pyo3_chrono::Duration,
        Option<pyo3_chrono::NaiveDateTime>,
    ) {
        ("TumblingWindowConfig", self.length, self.start_at)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration, Option<pyo3_chrono::NaiveDateTime>) {
        (pyo3_chrono::Duration(Duration::zero()), None)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TumblingWindowConfig", length, start_at)) = state.extract() {
            self.length = length;
            self.start_at = start_at;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TumblingWindowConfig: {state:?}"
            )))
        }
    }
}

/// Use fixed-length tumbling windows aligned to a start time.
pub(crate) struct TumblingWindower {
    length: Duration,
    start_at: NaiveDateTime,
    close_times: HashMap<WindowKey, NaiveDateTime>,
}

impl TumblingWindower {
    pub(crate) fn builder(
        length: Duration,
        start_at: NaiveDateTime,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Windower> {
        move |resume_state_bytes| {
            let close_times = resume_state_bytes.map(StateBytes::de).unwrap_or_default();

            Box::new(Self {
                length,
                start_at,
                close_times,
            })
        }
    }
}

impl Windower for TumblingWindower {
    fn insert(
        &mut self,
        watermark: &NaiveDateTime,
        item_time: NaiveDateTime,
    ) -> Vec<Result<WindowKey, InsertError>> {
        let since_start_at = item_time - self.start_at;
        let window_count = since_start_at.num_milliseconds() / self.length.num_milliseconds();

        let key = WindowKey(window_count);
        let close_at = self.start_at + self.length * (window_count as i32 + 1);

        if &close_at < watermark {
            vec![Err(InsertError::Late(key))]
        } else {
            self.close_times.insert(key, close_at);
            vec![Ok(key)]
        }
    }

    fn drain_closed(&mut self, watermark: &NaiveDateTime) -> Vec<WindowKey> {
        // TODO: Gosh I really want [`HashMap::drain_filter`].
        let mut future_close_times = HashMap::new();
        let mut closed_ids = Vec::new();

        for (id, close_at) in self.close_times.iter() {
            if close_at < watermark {
                closed_ids.push(*id);
            } else {
                future_close_times.insert(*id, *close_at);
            }
        }

        self.close_times = future_close_times;
        closed_ids
    }

    fn activate_after(&mut self, watermark: &NaiveDateTime) -> Option<Duration> {
        let watermark = *watermark;
        self.close_times
            .values()
            .cloned()
            .map(|t| t - watermark)
            .min()
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.close_times)
    }
}
