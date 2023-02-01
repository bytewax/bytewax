use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use pyo3::{pyclass, Python};

use crate::{add_pymethods, common::StringResult, window::WindowConfig};

use super::{Builder, InsertError, StateBytes, WindowBuilder, WindowKey, Windower};

/// Sliding windows of fixed duration.
///
/// Args:
///
///   length (datetime.timedelta): Length of window.
///
///   offset (datetime.timedelta): Offset between windows.
///
///   start_at (datetime.datetime): Instant of the first window. You
///       can use this to align all windows to an hour,
///       e.g. Defaults to system time of dataflow start.
///
/// Returns:
///
///   Config object. Pass this as the `window_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.config", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct SlidingWindowConfig {
    #[pyo3(get)]
    pub(crate) length: Duration,
    #[pyo3(get)]
    pub(crate) offset: Duration,
    #[pyo3(get)]
    pub(crate) start_at: Option<DateTime<Utc>>,
}

add_pymethods!(
    SlidingWindowConfig,
    parent: WindowConfig,
    py_args: (length, offset, start_at = "None"),
    args {
        length: Duration => Duration::zero(),
        offset: Duration => Duration::zero(),
        start_at: Option<DateTime<Utc>> => None
    }
);

impl WindowBuilder for SlidingWindowConfig {
    fn build(&self, _py: Python) -> StringResult<Builder> {
        Ok(Box::new(SlidingWindower::builder(
            self.length,
            self.offset,
            self.start_at.unwrap_or_else(Utc::now),
        )))
    }
}

pub(crate) struct SlidingWindower {
    length: Duration,
    offset: Duration,
    start_at: DateTime<Utc>,
    close_times: HashMap<WindowKey, DateTime<Utc>>,
}

impl SlidingWindower {
    pub(crate) fn builder(
        length: Duration,
        offset: Duration,
        start_at: DateTime<Utc>,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Windower> {
        move |resume_snapshot| {
            let close_times = resume_snapshot
                .map(StateBytes::de::<HashMap<WindowKey, DateTime<Utc>>>)
                .unwrap_or_default();
            Box::new(Self {
                length,
                offset,
                start_at,
                close_times,
            })
        }
    }
}

impl Windower for SlidingWindower {
    fn insert(
        &mut self,
        watermark: &DateTime<Utc>,
        item_time: &DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>> {
        let since_start_at = *item_time - self.start_at;
        let windows_count = since_start_at.num_milliseconds() / self.offset.num_milliseconds() + 1;
        let mut windows = vec![];
        // TODO: Avoid starting from 0 here, we can skip windows closed before the watermark
        for i in 0..windows_count {

            let key = WindowKey(i);
            let window_start =
                self.start_at + Duration::milliseconds(i * self.offset.num_milliseconds());
            let window_end = window_start + self.length;

            if window_end < *watermark {
                windows.push(Err(InsertError::Late(key)));
                continue;
            }

            if *item_time < window_start {
                continue;
            }
            if *item_time <= window_end {
                self.close_times
                    .entry(key)
                    .and_modify(|existing| {
                        assert!(
                            existing == &window_end,
                            "Sliding windower is not generating consistent boundaries"
                        )
                    })
                    .or_insert(window_end);
                windows.push(Ok(key));
            } else {
                windows.push(Err(InsertError::Late(key)));
            }
        }
        windows
    }

    fn get_close_times(&self) -> &HashMap<WindowKey, DateTime<Utc>> {
        &self.close_times
    }

    fn set_close_times(&mut self, close_times: HashMap<WindowKey, DateTime<Utc>>) {
        self.close_times = close_times;
    }
}
