use std::collections::HashMap;

use chrono::prelude::*;
use chrono::Duration;
use num::integer::Integer;
use pyo3::prelude::*;

use crate::{add_pymethods, window::WindowConfig};

use super::*;

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
///   length (datetime.timedelta):
///     Length of windows.
///   offset (datetime.timedelta):
///     Duration between start times of adjacent windows.
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
    signature: (length, offset, align_to),
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
    ) -> impl Fn(Option<TdPyAny>) -> Box<dyn Windower> {
        move |resume_snapshot| {
            let close_times = resume_snapshot
                .map(|snap| unwrap_any!(Python::with_gil(|py| snap.extract(py))))
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
        let first_window_idx = Integer::div_floor(
            &since_close_of_first_window.num_milliseconds(),
            &self.offset.num_milliseconds(),
        ) + 1;

        let num_windows = Integer::div_ceil(
            &self.length.num_milliseconds(),
            &self.offset.num_milliseconds(),
        );

        // Clone to not retain ownership of self in the closure.
        let time = *time;
        let align_to = self.align_to;
        let offset = self.offset;
        let length = self.length;
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
        let mut closed_keys = Vec::new();

        for (key, close_at) in self.close_times.iter() {
            if close_at < watermark {
                tracing::trace!("{key:?} closed at {close_at:?}");
                closed_keys.push(*key);
            } else {
                future_close_times.insert(*key, *close_at);
            }
        }

        self.close_times = future_close_times;
        closed_keys
    }

    fn is_empty(&self) -> bool {
        self.close_times.is_empty()
    }

    fn next_close(&self) -> Option<DateTime<Utc>> {
        self.close_times.values().min().cloned()
    }

    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.close_times.clone().into_py(py).into())
    }
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_bulk_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //              9:00:13
    //              I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 15).unwrap()
            ),
            (
                WindowKey(2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_bulk_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //             8:59:57
    //             I
    // [--------3)
    //      [--------2)
    //           [--------1)
    //                [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 57).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap()
            ),
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 5).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_bulk_zero_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //              9:00:03
    //              I
    // [--------2)
    //      [--------1)
    //           [0--------)
    //                [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 3).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 5).unwrap()
            ),
            (
                WindowKey(0),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_bulk_zero_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //             9:00:07
    //             I
    // [--------1)
    //      [0--------)
    //           [1--------)
    //                [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 7).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(0),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
            ),
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 15).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_edge_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                9:00:15
    //                I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    //                     [4--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 15).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap()
            ),
            (
                WindowKey(3),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 25).unwrap()
            ),
        ]
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_edge_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //           8:59:55
    //           I
    // [--------3)
    //      [--------2)
    //           [--------1)
    //                [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 55).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap()
            ),
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 05).unwrap()
            ),
        ]
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_edge_start_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //           9:00:00
    //           I
    // [--------2)
    //      [--------1)
    //           [0--------)
    //                [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 5).unwrap()
            ),
            (
                WindowKey(0),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
            ),
        ]
    );
}

#[test]
fn test_intersect_overlap_offset_divisible_by_length_edge_end_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //           9:00:10
    //           I
    // [0--------)
    //      [1--------)
    //           [2--------)
    //                [3--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 15).unwrap()
            ),
            (
                WindowKey(2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap()
            ),
        ]
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_bulk_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(3),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //            9:00:11
    //            I
    // [0--------)
    //    [1--------)
    //       [2--------)
    //          [3--------)
    //             [4--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 11).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap()
            ),
            (
                WindowKey(2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 16).unwrap()
            ),
            (
                WindowKey(3),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 19).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_bulk_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(3),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //            8:59:59
    //            I
    // [--------4)
    //    [--------3)
    //       [--------2)
    //          [--------1)
    //             [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 59).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-3),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 1).unwrap()
            ),
            (
                WindowKey(-2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 4).unwrap()
            ),
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 7).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_bulk_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(3),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //            9:00:05
    //            I
    // [--------2)
    //    [--------1)
    //       [0--------)
    //          [1--------)
    //             [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 5).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 7).unwrap()
            ),
            (
                WindowKey(0),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
            ),
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_start_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //               9:00:14
    //               I
    // [0--------)
    //        [1--------)
    //               [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 14).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 17).unwrap()
            ),
            (
                WindowKey(2),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 24).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_start_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //        8:59:53
    //        I
    // [--------2)
    //        [--------1)
    //               [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 53).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-2),
                Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 56).unwrap()
            ),
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 3).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_start_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //        9:00:00
    //        I
    // [--------1)
    //        [0--------)
    //               [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![
            (
                WindowKey(-1),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 3).unwrap()
            ),
            (
                WindowKey(0),
                Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
            ),
        ],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_end_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                  9:00:17
    //                  I
    // [0--------)
    //        [1--------)
    //               [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 17).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(2),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 24).unwrap()
        )],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_end_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //           8:59:56
    //           I
    // [--------2)
    //        [--------1)
    //               [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 56).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(-1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 3).unwrap()
        ),],
    );
}

#[test]
fn test_intersect_overlap_offset_indivisible_by_length_edge_end_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(7),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                  9:00:10
    //                  I
    // [--------1)
    //        [0--------)
    //               [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 17).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_bulk_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                9:00:15
    //                I
    // [0--------)
    //           [1--------)
    //                     [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 15).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_bulk_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                8:59:55
    //                I
    // [--------2)
    //           [--------1)
    //                     [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 55).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(-1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_bulk_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                9:00:05
    //                I
    // [--------1)
    //           [0--------)
    //                     [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 5).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(0),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_edge_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                     9:00:20
    //                     I
    // [0--------)
    //           [1--------)
    //                     [2--------)
    //                               [3--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(2),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 30).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_edge_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                     8:59:50
    //                     I
    // [--------3)
    //           [--------2)
    //                     [--------1)
    //                               [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 50).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(-1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_edge_zero_start() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //           9:00:00
    //           I
    // [--------1)
    //           [0--------)
    //                     [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(0),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
        )],
    );
}

#[test]
fn test_intersect_tumble_edge_zero_end() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(10),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                     9:00:10
    //                     I
    // [--------1)
    //           [0--------)
    //                     [1--------)
    //                               [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 20).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_bulk_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                   9:00:18
    //                   I
    // [0--------)
    //              [1--------)
    //                           [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 18).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 23).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_bulk_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                   8:59:48
    //                   I
    // [--------2)
    //              [--------1)
    //                           [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 48).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(-1),
            Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 57).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_bulk_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                   9:00:03
    //                   I
    // [--------1)
    //              [0--------)
    //                           [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 3).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(0),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_gap_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //             9:00:20
    //             I
    // [0--------)
    //              [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 12).unwrap();
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_intersect_gap_gap_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //             8:59:59
    //             I
    // [--------1)
    //              [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 59).unwrap();
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_intersect_gap_edge_start_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //              9:00:13
    //              I
    // [0--------)
    //              [1--------)
    //                           [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(1),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 23).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_edge_start_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //              8:59:47
    //              I
    // [--------2)
    //              [--------1)
    //                           [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 47).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(-1),
            Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 57).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_edge_start_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //              9:00:00
    //              I
    // [--------1)
    //              [0--------)
    //                           [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap();
    assert_eq!(
        windower.intersects(&item_time).collect::<Vec<_>>(),
        vec![(
            WindowKey(0),
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap()
        )],
    );
}

#[test]
fn test_intersect_gap_edge_end_positive() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                        9:00:23
    //                        I
    // [0--------)
    //              [1--------)
    //                           [2--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 23).unwrap();
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_intersect_gap_edge_end_negative() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                        8:59:57
    //                        I
    // [--------2)
    //              [--------1)
    //                           [0--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 57).unwrap();
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_intersect_gap_edge_end_zero() {
    let windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(13),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
        close_times: HashMap::new(),
    };

    //                        9:00:10
    //                        I
    // [--------1)
    //              [0--------)
    //                           [1--------)
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 10).unwrap();
    assert_eq!(windower.intersects(&item_time).collect::<Vec<_>>(), vec![]);
}

#[test]
fn test_insert() {
    let mut windower = SlidingWindower {
        length: Duration::seconds(10),
        offset: Duration::seconds(5),
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
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
    let watermark = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 17).unwrap();
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap();
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
        align_to: Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0).unwrap(),
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
    let watermark1 = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 04).unwrap();
    let item_time = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 13).unwrap();
    let _ = windower.insert(&watermark1, &item_time);

    let watermark2 = Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 17).unwrap();
    assert_eq!(windower.drain_closed(&watermark2), vec![WindowKey(1)]);
}
