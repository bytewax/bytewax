use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use pyo3::prelude::*;

use crate::{add_pymethods, window::WindowConfig};

use super::{Builder, InsertError, StateBytes, WindowBuilder, WindowKey, Windower};

/// Hopping windows of fixed duration.
/// If offset == length, you get tumbling windows.
/// If offset < length, windows overlap.
/// If offset > length, there will be holes between windows.
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
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct HoppingWindowConfig {
    #[pyo3(get)]
    pub(crate) length: Duration,
    #[pyo3(get)]
    pub(crate) offset: Duration,
    #[pyo3(get)]
    pub(crate) start_at: Option<DateTime<Utc>>,
}

add_pymethods!(
    HoppingWindowConfig,
    parent: WindowConfig,
    py_args: (length, offset, start_at = "None"),
    args {
        length: Duration => Duration::zero(),
        offset: Duration => Duration::zero(),
        start_at: Option<DateTime<Utc>> => None
    }
);

impl WindowBuilder for HoppingWindowConfig {
    fn build(&self, _py: Python) -> PyResult<Builder> {
        Ok(Box::new(HoppingWindower::builder(
            self.length,
            self.offset,
            self.start_at.unwrap_or_else(Utc::now),
        )))
    }
}

pub(crate) struct HoppingWindower {
    length: Duration,
    offset: Duration,
    start_at: DateTime<Utc>,
    close_times: HashMap<WindowKey, DateTime<Utc>>,
}

impl HoppingWindower {
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

    fn add_close_time(&mut self, key: WindowKey, window_end: DateTime<Utc>) {
        self.close_times
            .entry(key)
            .and_modify(|existing| {
                assert!(
                    existing == &window_end,
                    "HoppingWindower is not generating consistent boundaries"
                )
            })
            .or_insert(window_end);
    }
}

impl Windower for HoppingWindower {
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
                // First generate the WindowKey and calculate
                // the window_end time
                let key = WindowKey(i);
                let window_start = self.start_at + Duration::milliseconds(i * offset);
                let window_end = window_start + self.length;
                // We only want to add items that happened between
                // start and end of the window.
                // If the watermark is past the end of the window,
                // any item is late for this window.
                if *item_time >= window_start && *item_time < window_end && *watermark <= window_end
                {
                    self.add_close_time(key, window_end);
                    Ok(key)
                } else {
                    // We send `Late` even if the item came too early,
                    // maybe we should differentate
                    Err(InsertError::Late(key))
                }
            })
            .collect()
    }

    /// Look at the current watermark, determine which windows are now
    /// closed, return them, and remove them from internal state.
    fn drain_closed(&mut self, watermark: &DateTime<Utc>) -> Vec<WindowKey> {
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

    fn is_empty(&self) -> bool {
        self.close_times.is_empty()
    }

    fn next_close(&self) -> Option<DateTime<Utc>> {
        self.close_times.values().cloned().min()
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<HashMap<WindowKey, DateTime<Utc>>>(&self.close_times)
    }
}
