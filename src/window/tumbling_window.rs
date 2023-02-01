use std::collections::HashMap;

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
pub(crate) struct TumblingWindowConfig {
    #[pyo3(get)]
    pub(crate) length: chrono::Duration,
    #[pyo3(get)]
    pub(crate) start_at: Option<DateTime<Utc>>,
}

impl WindowBuilder for TumblingWindowConfig {
    fn build(&self, _py: Python) -> StringResult<Builder> {
        Ok(Box::new(TumblingWindower::builder(
            self.length,
            self.start_at.unwrap_or_else(Utc::now),
        )))
    }
}

add_pymethods!(
    TumblingWindowConfig,
    parent: WindowConfig,
    py_args: (length, start_at = "None"),
    args {
        length: Duration => Duration::zero(),
        start_at: Option<DateTime<Utc>> => None
    }
);

/// Use fixed-length tumbling windows aligned to a start time.
pub(crate) struct TumblingWindower {
    length: Duration,
    start_at: DateTime<Utc>,
    close_times: HashMap<WindowKey, DateTime<Utc>>,
}

impl TumblingWindower {
    pub(crate) fn builder(
        length: Duration,
        start_at: DateTime<Utc>,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Windower> {
        move |resume_snapshot| {
            let close_times = resume_snapshot
                .map(StateBytes::de::<HashMap<WindowKey, DateTime<Utc>>>)
                .unwrap_or_default();

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
        watermark: &DateTime<Utc>,
        item_time: &DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>> {
        let since_start_at = *item_time - self.start_at;
        let window_count = since_start_at.num_milliseconds() / self.length.num_milliseconds();

        let key = WindowKey(window_count);
        let close_at = self
            .start_at
            .checked_add_signed(self.length * (window_count as i32 + 1))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);

        if &close_at < watermark {
            vec![Err(InsertError::Late(key))]
        } else {
            self.close_times
                .entry(key)
                .and_modify(|existing_close_at| {
                    assert!(
                        existing_close_at == &close_at,
                        "Tumbling windower is not generating consistent boundaries"
                    )
                })
                .or_insert(close_at);
            vec![Ok(key)]
        }
    }

    fn get_close_times(&self) -> &HashMap<WindowKey, DateTime<Utc>> {
        &self.close_times
    }

    fn set_close_times(&mut self, close_times: HashMap<WindowKey, DateTime<Utc>>) {
        self.close_times = close_times;
    }
}
