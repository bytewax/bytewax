use std::task::Poll;

use chrono::prelude::*;
use chrono::Duration;
use pyo3::prelude::*;

use crate::{
    add_pymethods,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    window::clock::ClockConfig,
};

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

impl ClockBuilder<TdPyAny> for EventClockConfig {
    fn build(&self, py: Python) -> PyResult<Builder<TdPyAny>> {
        let dt_getter = self.dt_getter.clone_ref(py);
        let wait_for_system_duration = self.wait_for_system_duration.clone();

        Ok(Box::new(move |resume_snapshot: Option<StateBytes>| {
            let max_event_time_system_time =
                resume_snapshot.map(StateBytes::de).unwrap_or_default();

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

add_pymethods!(
    EventClockConfig,
    parent: ClockConfig,
    py_args: (dt_getter, wait_for_system_duration),
    args {
        dt_getter: TdPyCallable => Python::with_gil(TdPyCallable::pickle_new),
        wait_for_system_duration: Duration => Duration::zero()
    }
);

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
        self.now.clone()
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

        let (max_event_time_system_time, watermark) =
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
                .max_by_key(|(_, watermark)| *watermark)
                .unzip();

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

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.max_event_time_system_time)
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
                now: Utc.ymd(2023, 5, 10).and_hms(9, 0, 0),
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

        let item1 = Python::with_gil(|py| Utc.ymd(2023, 3, 16).and_hms(9, 0, 0).into_py(py).into());
        let found = clock.watermark(&Poll::Ready(Some(item1)));
        assert_eq!(
            found,
            Utc.ymd(2023, 3, 16).and_hms(8, 59, 50),
            "watermark should be item time minus late time"
        );

        clock.source.now = Utc.ymd(2023, 5, 10).and_hms(9, 0, 2);
        let found = clock.watermark(&Poll::Pending);
        assert_eq!(
            found,
            Utc.ymd(2023, 3, 16).and_hms(8, 59, 52),
            "watermark should forward by system time between awakenings if no new items"
        );

        clock.source.now = Utc.ymd(2023, 5, 10).and_hms(9, 0, 4);
        let item2 = Python::with_gil(|py| Utc.ymd(2023, 3, 16).and_hms(9, 1, 0).into_py(py).into());
        let found = clock.watermark(&Poll::Ready(Some(item2)));
        assert_eq!(
            found,
            Utc.ymd(2023, 3, 16).and_hms(9, 0, 50),
            "watermark should advance to item time minus late time if new item"
        );

        clock.source.now = Utc.ymd(2023, 5, 10).and_hms(9, 0, 6);
        let item3 = Python::with_gil(|py| Utc.ymd(2023, 3, 16).and_hms(9, 1, 1).into_py(py).into());
        let found = clock.watermark(&Poll::Ready(Some(item3)));
        assert_eq!(
            found,
            Utc.ymd(2023, 3, 16).and_hms(9, 0, 52),
            "watermark should not go backwards due to slow item"
        );

        clock.source.now = Utc.ymd(2023, 5, 10).and_hms(9, 0, 8);
        let found = clock.watermark(&Poll::Ready(None));
        assert_eq!(
            found,
            DateTime::<Utc>::MAX_UTC,
            "watermark should advance to the end of time when input ends"
        );

        clock.source.now = Utc.ymd(2023, 5, 10).and_hms(9, 0, 10);
        let found = clock.watermark(&Poll::Ready(None));
        assert_eq!(
            found,
            DateTime::<Utc>::MAX_UTC,
            "watermark should saturate to the end of time after input ends"
        );
    }

    #[test]
    fn test_event_time_clock_serialization() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let config = EventClockConfig {
                // This will never be called in this test so can be
                // junk.
                dt_getter: TdPyCallable::pickle_new(py),
                wait_for_system_duration: Duration::zero(),
            };

            let mut event_clock = config.build(py).unwrap()(None);

            let found = event_clock.watermark(&Poll::Ready(None));
            assert_eq!(
                found,
                DateTime::<Utc>::MAX_UTC,
                "watermark did not advance to the end of time when input ends"
            );

            let snapshot = event_clock.snapshot();

            let mut event_clock = config.build(py).unwrap()(Some(snapshot));

            let found = event_clock.watermark(&Poll::Pending);
            assert_eq!(
                found,
                DateTime::<Utc>::MAX_UTC,
                "clock built from snapshot did not restore state"
            );
        });
    }
}
