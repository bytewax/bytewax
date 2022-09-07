use std::task::Poll;

use chrono::{Duration, NaiveDateTime};
use pyo3::{exceptions::PyValueError, prelude::*};

use super::{Clock, ClockConfig};
use crate::recovery::StateBytes;

/// Use to simulate system time in tests. Increment "now" after each
/// item.
///
/// If the dataflow has no more input, all windows are closed.
///
/// The watermark uses the most recent "now".
///
/// Args:
///
///   item_incr (datetime.timedelta): Amount to increment "now"
///       after each item.
///
///   start_at (datetime.datetime): Initial "now" / time of first
///       item. If you set this and use a window config
///
/// Returns:
///
///   Config object. Pass this as the `clock_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(item_incr)")]
pub(crate) struct TestingClockConfig {
    #[pyo3(get)]
    pub(crate) item_incr: pyo3_chrono::Duration,
    #[pyo3(get)]
    pub(crate) start_at: pyo3_chrono::NaiveDateTime,
}

#[pymethods]
impl TestingClockConfig {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args(item_incr, start_at)]
    fn new(
        item_incr: pyo3_chrono::Duration,
        start_at: pyo3_chrono::NaiveDateTime,
    ) -> (Self, ClockConfig) {
        (
            Self {
                item_incr,
                start_at,
            },
            ClockConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, pyo3_chrono::Duration, pyo3_chrono::NaiveDateTime) {
        ("TestingClockConfig", self.item_incr, self.start_at)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::Duration, pyo3_chrono::NaiveDateTime) {
        (
            pyo3_chrono::Duration(Duration::zero()),
            pyo3_chrono::NaiveDateTime(chrono::naive::MAX_DATETIME),
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClockConfig", item_incr, start_at)) = state.extract() {
            self.item_incr = item_incr;
            self.start_at = start_at;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TestingClockConfig: {state:?}"
            )))
        }
    }
}

/// Simulate system time for tests. Increment "now" after each item.
pub(crate) struct TestingClock {
    item_incr: Duration,
    current_time: NaiveDateTime,
}

impl TestingClock {
    pub(crate) fn builder<V>(
        item_incr: Duration,
        start_at: NaiveDateTime,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Clock<V>> {
        move |resume_state_bytes: Option<StateBytes>| {
            let current_time = resume_state_bytes.map(StateBytes::de).unwrap_or(start_at);

            Box::new(Self {
                item_incr,
                current_time,
            })
        }
    }
}

impl<V> Clock<V> for TestingClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => self.current_time,
        }
    }

    fn time_for(&mut self, _item: &V) -> NaiveDateTime {
        let item_time = self.current_time;
        self.current_time += self.item_incr;
        item_time
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.current_time)
    }
}

#[test]
fn test_testing_clock() {
    use chrono::{NaiveDate, NaiveTime};

    let mut clock = TestingClock {
        item_incr: Duration::seconds(1),
        current_time: NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 0, 0),
        ),
    };
    assert_eq!(
        clock.time_for(&"x"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 0, 0)
        )
    );
    assert_eq!(
        clock.time_for(&"y"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 1, 0)
        )
    );
    assert_eq!(
        clock.time_for(&"z"),
        NaiveDateTime::new(
            NaiveDate::from_ymd(2022, 1, 1),
            NaiveTime::from_hms_milli(0, 0, 2, 0)
        )
    );
}
