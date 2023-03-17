use std::collections::HashMap;

use chrono::prelude::*;
use chrono::Duration;
use pyo3::prelude::*;

use crate::{add_pymethods, window::WindowConfig};

use super::{Builder, InsertError, StateBytes, WindowBuilder, WindowKey, Windower};

/// Sliding windows of fixed duration.
///
/// If offset == length, windows cover all time but do not
/// overlap. Each item will fall in exactly one window. The
/// `TumblingWindow` config will do this for you.
///
/// If offset < length, windows overlap. Each item will fall in
/// multiple windows.
///
/// If offset > length, there will be gaps between windows. Each item
/// can fall in up to one window, but might fall into none.
///
/// Window start times are inclusive, but end times are exclusive.
///
/// Args:
///
///   length (datetime.timedelta): Length of windows.
///
///   offset (datetime.timedelta): Duration between start times of
///     adjacent windows.
///
///   align_to (datetime.datetime): Align windows so this instant
///     starts a window. You can use this to align all windows to hour
///     boundaries, e.g.
///
/// Returns:
///
///   Config object. Pass this as the `window_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=WindowConfig)]
#[derive(Clone)]
pub(crate) struct SlidingWindow {
    #[pyo3(get)]
    pub(crate) length: Duration,
    #[pyo3(get)]
    pub(crate) offset: Duration,
    #[pyo3(get)]
    pub(crate) align_to: DateTime<Utc>,
}

add_pymethods!(
    SlidingWindow,
    parent: WindowConfig,
    py_args: (length, offset, align_to),
    args {
        length: Duration => Duration::zero(),
        offset: Duration => Duration::zero(),
        align_to: DateTime<Utc> => DateTime::<Utc>::MIN_UTC
    }
);

impl WindowBuilder for SlidingWindow {
    fn build(&self, _py: Python) -> PyResult<Builder> {
        Ok(Box::new(SlidingWindower::builder(
            self.length,
            self.offset,
            self.align_to,
        )))
    }
}

pub(crate) struct SlidingWindower {
    length: Duration,
    offset: Duration,
    align_to: DateTime<Utc>,
    close_times: HashMap<WindowKey, DateTime<Utc>>,
}

impl SlidingWindower {
    pub(crate) fn builder(
        length: Duration,
        offset: Duration,
        align_to: DateTime<Utc>,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Windower> {
        move |resume_snapshot| {
            let close_times = resume_snapshot
                .map(StateBytes::de::<HashMap<WindowKey, DateTime<Utc>>>)
                .unwrap_or_default();
            Box::new(Self {
                length,
                offset,
                align_to,
                close_times,
            })
        }
    }

    /// Yields all windows and their close times that intersect a
    /// given time. Close time is exclusive.
    fn intersects(&self, time: &DateTime<Utc>) -> impl Iterator<Item = (WindowKey, DateTime<Utc>)> {
        let since_close_of_first_window = *time - (self.align_to + self.length);
        let first_window_idx = since_close_of_first_window
            .num_milliseconds()
            // We always want to round towards -inf. TODO:
            // i64::div_floor will more obviously do this.
            .div_euclid(self.offset.num_milliseconds())
            + 1;

        // Round up to 1 in case we have offset > length. Always try
        // at least one window. We might filter it out, though.
        let num_windows = std::cmp::max(
            self.length.num_milliseconds() / self.offset.num_milliseconds(),
            1,
        );

        // Clone to not retain ownership of self in the closure.
        let time = time.clone();
        let align_to = self.align_to.clone();
        let offset = self.offset.clone();
        let length = self.length.clone();
        (0..num_windows).flat_map(move |i| {
            let window_idx = first_window_idx + i;
            let window_open = align_to + offset * window_idx as i32;
            if time < window_open {
                None
            } else {
                let window_close = window_open + length;
                Some((WindowKey(window_idx), window_close))
            }
        })
    }

    fn insert_window(&mut self, key: WindowKey, close_time: DateTime<Utc>) {
        self.close_times
            .entry(key)
            .and_modify(|existing| {
                assert!(
                    existing == &close_time,
                    "SlidingWindower is not generating consistent boundaries"
                )
            })
            .or_insert(close_time);
    }
}

impl Windower for SlidingWindower {
    fn insert(
        &mut self,
        watermark: &DateTime<Utc>,
        item_time: &DateTime<Utc>,
    ) -> Vec<Result<WindowKey, InsertError>> {
        self.intersects(item_time)
            .map(|(key, close_time)| {
                tracing::trace!("Intersects with {key:?} closing at {close_time:?}");
                if close_time < *watermark {
                    Err(InsertError::Late(key))
                } else {
                    self.insert_window(key, close_time);
                    Ok(key)
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
        self.close_times.values().min().cloned()
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<HashMap<WindowKey, DateTime<Utc>>>(&self.close_times)
    }
}

#[test]
fn test_intersect_overlap_bulk_offset_divisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //              9:00:13
    //              I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 13);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 15)),
            (WindowKey(2), Utc.ymd(2023, 3, 16).and_hms(9, 0, 20)),
        ],
    );
}

#[test]
fn test_intersect_overlap_negative_offset_divisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //   9:00:01
    //   I
    // ----1)
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 1);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (WindowKey(-1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 5)),
            (WindowKey(0), Utc.ymd(2023, 3, 16).and_hms(9, 0, 10)),
        ],
    );
}

#[test]
fn test_intersect_overlap_bulk_offset_indivisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(3),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //            9:00:11
    //            I
    // [0--------)
    //    [1--------)
    //       [2--------)
    //          [3--------)
    //             [4--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 11);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 13)),
            (WindowKey(2), Utc.ymd(2023, 3, 16).and_hms(9, 0, 16)),
            (WindowKey(3), Utc.ymd(2023, 3, 16).and_hms(9, 0, 19)),
        ],
    );
}

#[test]
fn test_intersect_overlap_negative_offset_indivisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(3),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //   9:00:02
    //   I
    // 3)
    // ---2)
    // ------1)
    // [0--------)
    //    [1--------)
    //       [2--------)
    //          [3--------)
    //             [4--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 2);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (WindowKey(-2), Utc.ymd(2023, 3, 16).and_hms(9, 0, 4)),
            (WindowKey(-1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 7)),
            (WindowKey(0), Utc.ymd(2023, 3, 16).and_hms(9, 0, 10)),
        ],
    );
}

#[test]
fn test_intersect_overlap_end_edge_offset_divisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //           9:00:10
    //           I
    // [0--------)
    //        [1--------)
    //               [2--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 10);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 17))],
    );
}

#[test]
fn test_intersect_overlap_start_edge_offset_divisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //        9:00:07
    //        I
    // [0--------)
    //        [1--------)
    //               [2--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 7);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(0), Utc.ymd(2023, 3, 16).and_hms(9, 0, 10)),],
    );
}

#[test]
fn test_intersect_overlap_edge_offset_divisible_by_length() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //           9:00:10
    //           I
    // [0--------)
    //      [1--------)
    //           [2--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 10);
    let found: Vec<_> = windower.intersects(&item_time).collect();
    assert_eq!(found.len(), 2);
    assert_eq!(
        found[0],
        (WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 15)),
        "intersect did not consider window end exclusive"
    );
    assert_eq!(
        found[1],
        (WindowKey(2), Utc.ymd(2023, 3, 16).and_hms(9, 0, 20)),
        "intersect did not consider window start inclusive"
    );
}

#[test]
fn test_intersect_tumble_bulk() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //         9:00:08
    //         I
    // [0--------)
    //           [1--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 8);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(0), Utc.ymd(2023, 3, 16).and_hms(9, 0, 10))],
    );
}

#[test]
fn test_intersect_tumble_edge() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //           9:00:10
    //           I
    // [0--------)
    //           [1--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 10);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 20))],
    );
}

#[test]
fn test_intersect_gap_bulk() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(15),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //         9:00:08
    //         I
    // [0--------)
    //                [1--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 8);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(0), Utc.ymd(2023, 3, 16).and_hms(9, 0, 10))],
    );
}

#[test]
fn test_intersect_gap_end_edge() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(15),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //           9:00:10
    //           I
    // [0--------)
    //                [1--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 10);
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_intersect_gap_start_edge() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(15),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //                9:00:15
    //                I
    // [0--------)
    //                [1--------)
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 15);
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(WindowKey(1), Utc.ymd(2023, 3, 16).and_hms(9, 0, 25))],
    );
}

#[test]
fn test_insert() {
    let mut windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //                  9:00:17
    //                  W
    //              9:00:13
    //              I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let watermark = Utc.ymd(2023, 3, 16).and_hms(9, 0, 17);
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 13);
    assert_eq!(
        windower.insert(&watermark, &item_time),
        vec![Err(InsertError::Late(WindowKey(1))), Ok(WindowKey(2))]
    );
}

#[test]
fn test_drain_closed() {
    let mut windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.ymd(2023, 3, 16).and_hms(9, 0, 0),
        close_times: HashMap::new(),
    };

    //     9:00:04      9:00:17
    //     W1           W2
    //              9:00:13
    //              I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let watermark1 = Utc.ymd(2023, 3, 16).and_hms(9, 0, 04);
    let item_time = Utc.ymd(2023, 3, 16).and_hms(9, 0, 13);
    let _ = windower.insert(&watermark1, &item_time);

    let watermark2 = Utc.ymd(2023, 3, 16).and_hms(9, 0, 17);
    assert_eq!(windower.drain_closed(&watermark2), vec![WindowKey(1)]);
}
