use std::task::Poll;

use chrono::prelude::*;
use chrono::Duration;
use pyo3::prelude::*;

use crate::pyo3_extensions::TdPyAny;
use crate::pyo3_extensions::TdPyCallable;
use crate::unwrap_any;
use crate::window::clock::ClockConfig;

use super::*;

/// Use a getter function to lookup the timestamp for each item.
///
/// The watermark is the largest item timestamp seen thus far, minus
/// the waiting duration, plus the system time duration that has
/// elapsed since that item was seen. This effectively means items
/// will be correctly processed as long as they are not out of order
/// more than the waiting duration in system time.
///
/// If the dataflow has no more input, all windows are closed.
///
/// Args:
///
///   dt_getter: Python function to get a datetime from an event. The
///     datetime returned must have tzinfo set to
///     `timezone.utc`. E.g. `datetime(1970, 1, 1,
///     tzinfo=timezone.utc)`
///
///   wait_for_system_duration: How much system time to wait before
///     considering an event late.
///
/// Returns:
///
///   Config object. Pass this as the `clock_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[derive(Clone)]
pub(crate) struct EventClockConfig {
    #[pyo3(get)]
    pub(crate) dt_getter: TdPyCallable,
    #[pyo3(get)]
    pub(crate) wait_for_system_duration: Duration,
}

#[pymethods]
impl EventClockConfig {
    #[new]
    fn new(dt_getter: TdPyCallable, wait_for_system_duration: Duration) -> (Self, ClockConfig) {
        let self_ = Self {
            dt_getter,
            wait_for_system_duration,
        };
        let super_ = ClockConfig::new();
        (self_, super_)
    }
}

impl ClockBuilder<TdPyAny> for EventClockConfig {
    fn build(&self, py: Python) -> PyResult<Builder<TdPyAny>> {
        let dt_getter = self.dt_getter.clone_ref(py);
        let wait_for_system_duration = self.wait_for_system_duration;

        Ok(Box::new(move |resume_snapshot: Option<TdPyAny>| {
            let max_event_time_system_time = resume_snapshot
                .map(|snap| unwrap_any!(Python::with_gil(|py| snap.extract(py))))
                .unwrap_or_default();

            let dt_getter = dt_getter.clone();
            Box::new(EventClock {
                source: SystemTimeSource {},
                dt_getter,
                wait_for_system_duration,
                max_event_time_system_time,
            })
        }))
    }
}

/// A little mock point for getting system time.
trait TimeSource {
    fn now(&self) -> DateTime<Utc>;
}

/// Get the actual system time.
struct SystemTimeSource {}

impl TimeSource for SystemTimeSource {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Use a fixed system time.
struct TestTimeSource {
    now: DateTime<Utc>,
}

impl TimeSource for TestTimeSource {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }
}

pub(crate) struct EventClock<S> {
    source: S,
    dt_getter: TdPyCallable,
    wait_for_system_duration: Duration,
    /// The largest event time we've seen and the system time we saw
    /// that event at.
    max_event_time_system_time: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl<S> Clock<TdPyAny> for EventClock<S>
where
    S: TimeSource,
{
    fn watermark(&mut self, next_value: &Poll<Option<TdPyAny>>) -> DateTime<Utc> {
        let now = self.source.now();

        // Incoming event time and system time it was ingested.
        let next_event_time_system_time = match next_value {
            Poll::Ready(Some(event)) => Some((self.time_for(event), now)),
            Poll::Ready(None) => Some((DateTime::<Utc>::MAX_UTC, now)),
            Poll::Pending => None,
        };

        let res =
            // Re-calc the current watermark using the item that
            // previously produced the largest watermark, and also the
            // watermark from the current new item (if any).
            [self.max_event_time_system_time, next_event_time_system_time]
                .into_iter()
                .flatten()
                .map(|(event_time, event_system_time)| {
                    let system_duration_since_max_event =
                        now.signed_duration_since(event_system_time);
                    // If we're at the end of input, force watermark
                    // to end also. Otherwise, subtract the late
                    // "waiting duration" and progress it forward as
                    // system time flows on.
                    let watermark = if event_time == DateTime::<Utc>::MAX_UTC {
                        DateTime::<Utc>::MAX_UTC
                    } else {
                        event_time - self.wait_for_system_duration + system_duration_since_max_event
                    };

                    ((event_time, event_system_time), watermark)
                })
            // Then find the max watermark. If the new item would
            // cause the watermark to go backwards, we'll filter it
            // out here.
            .max_by_key(|(_, watermark)| *watermark);
        // TODO: Could replace with [`Option::unzip`] when stable.
        let (max_event_time_system_time, watermark) = match res {
            Some((et_st, watermark)) => (Some(et_st), Some(watermark)),
            None => (None, None),
        };

        self.max_event_time_system_time = max_event_time_system_time;
        // If we've seen no items yet, the watermark is infinitely far
        // in the past since we have no idea what time the data will
        // start at.
        watermark.unwrap_or(DateTime::<Utc>::MIN_UTC)
    }

    fn time_for(&mut self, event: &TdPyAny) -> DateTime<Utc> {
        Python::with_gil(|py| {
            self.dt_getter
                // Call the event time getter function with the event as parameter
                .call1(py, (event.clone_ref(py),))
                .unwrap()
                // Convert to DateTime<Utc>
                .extract(py)
                .unwrap()
        })
    }

    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.max_event_time_system_time.into_py(py).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark() {
        pyo3::prepare_freethreaded_python();

        // We're going to pass in datetimes as the items themselves.
        let identity = Python::with_gil(|py| {
            py.eval("lambda x: x", None, None)
                .unwrap()
                .extract()
                .unwrap()
        });

        let mut clock = EventClock {
            source: TestTimeSource {
                now: Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 0).unwrap(),
            },
            dt_getter: identity,
            wait_for_system_duration: Duration::seconds(10),
            max_event_time_system_time: None,
        };

        let found = clock.watermark(&Poll::Pending);
        assert_eq!(
            found,
            DateTime::<Utc>::MIN_UTC,
            "watermark should start at the beginning of time before any items"
        );

        let item1 = Python::with_gil(|py| {
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 0)
                .unwrap()
                .into_py(py)
                .into()
        });
        let found = clock.watermark(&Poll::Ready(Some(item1)));
        assert_eq!(
            found,
            Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 50).unwrap(),
            "watermark should be item time minus late time"
        );

        clock.source.now = Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 2).unwrap();
        let found = clock.watermark(&Poll::Pending);
        assert_eq!(
            found,
            Utc.with_ymd_and_hms(2023, 3, 16, 8, 59, 52).unwrap(),
            "watermark should forward by system time between awakenings if no new items"
        );

        clock.source.now = Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 4).unwrap();
        let item2 = Python::with_gil(|py| {
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 1, 0)
                .unwrap()
                .into_py(py)
                .into()
        });
        let found = clock.watermark(&Poll::Ready(Some(item2)));
        assert_eq!(
            found,
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 50).unwrap(),
            "watermark should advance to item time minus late time if new item"
        );

        clock.source.now = Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 6).unwrap();
        let item3 = Python::with_gil(|py| {
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 1, 1)
                .unwrap()
                .into_py(py)
                .into()
        });
        let found = clock.watermark(&Poll::Ready(Some(item3)));
        assert_eq!(
            found,
            Utc.with_ymd_and_hms(2023, 3, 16, 9, 0, 52).unwrap(),
            "watermark should not go backwards due to slow item"
        );

        clock.source.now = Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 8).unwrap();
        let found = clock.watermark(&Poll::Ready(None));
        assert_eq!(
            found,
            DateTime::<Utc>::MAX_UTC,
            "watermark should advance to the end of time when input ends"
        );

        clock.source.now = Utc.with_ymd_and_hms(2023, 5, 10, 9, 0, 10).unwrap();
        let found = clock.watermark(&Poll::Ready(None));
        assert_eq!(
            found,
            DateTime::<Utc>::MAX_UTC,
            "watermark should saturate to the end of time after input ends"
        );
    }
}
