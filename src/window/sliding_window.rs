use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use pyo3::{pyclass, Python};

use crate::{add_pymethods, common::StringResult, window::WindowConfig};

use super::{Builder, InsertError, StateBytes, WindowBuilder, WindowKey, Windower};

/// Sliding windows of fixed duration.
/// Windows overlap if offset < length, and leave holes if offset > length.
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
        let since_start_at = (*item_time - self.start_at).num_milliseconds();
        let since_watermark = (*watermark - self.start_at).num_milliseconds();
        let offset = self.offset.num_milliseconds();

        let windows_count = since_start_at / offset + 1;
        let first_window = (since_watermark / offset - 1).max(0);

        (first_window..windows_count)
            .map(|i| {
                // First generate the WindowKey and calculate the window_end time
                let key = WindowKey(i);
                let window_start = self.start_at + Duration::milliseconds(i * offset);
                let window_end = window_start + self.length;
                // We only want to add items that happened between start and end of the window.
                // If the watermark is past the end of the window, any item is late for this
                // window.
                if *item_time <= window_end
                    && *item_time >= window_start
                    && *watermark <= window_end
                {
                    self.add_close_time(key, window_end);
                    Ok(key)
                } else {
                    // We send `Late` even if the item came too early, maybe we should differentate
                    Err(InsertError::Late(key))
                }
            })
            .collect()
    }

    fn get_close_times(&self) -> &HashMap<WindowKey, DateTime<Utc>> {
        &self.close_times
    }

    fn get_close_times_mut(&mut self) -> &mut HashMap<WindowKey, DateTime<Utc>> {
        &mut self.close_times
    }

    fn set_close_times(&mut self, close_times: HashMap<WindowKey, DateTime<Utc>>) {
        self.close_times = close_times;
    }
}
