use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyAny, PyResult, Python};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::StateBytes,
    window::ClockConfig,
};

use super::{Builder, Clock, ClockBuilder};

/// Use datetimes from events as clock.
///
/// If the dataflow has no more input, all windows are closed.
///
/// The watermark is the system time since the last element
/// plus the value of `late` plus the delay of the latest received element.
/// It is updated every time an event with a newer datetime is processed.
///
/// Args:
///
///   dt_getter: Python function to get a datetime from an event.
///
///   wait_for_system_duration: How much (system) time to wait before considering an event late.
///
/// Returns:
///   Config object. Pass this as the `clock_config` parameter to your
///   windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(dt_getter, wait_for_system_duration)")]
#[derive(Clone)]
pub(crate) struct EventClockConfig {
    #[pyo3(get)]
    pub(crate) dt_getter: TdPyCallable,
    #[pyo3(get)]
    pub(crate) wait_for_system_duration: chrono::Duration,
}

impl ClockBuilder<TdPyAny> for EventClockConfig {
    fn builder(self) -> Builder<TdPyAny> {
        Box::new(move |resume_snapshot: Option<StateBytes>| {
            // Deserialize data if a snapshot existed
            let (latest_event_time, system_time_of_last_event, late_time, watermark) =
                resume_snapshot.map(StateBytes::de).unwrap_or_else(|| {
                    // Defaults values
                    (
                        None,
                        // system_time_of_last_event
                        Utc::now(),
                        // late_time
                        DateTime::<Utc>::MIN_UTC,
                        // watermark
                        DateTime::<Utc>::MIN_UTC,
                    )
                });

            Box::new(EventClock::new(
                self.dt_getter.clone(),
                self.wait_for_system_duration,
                latest_event_time,
                late_time,
                system_time_of_last_event,
                watermark,
            ))
        })
    }
}

#[pymethods]
impl EventClockConfig {
    #[new]
    #[args(dt_getter, late, test)]
    fn new(
        dt_getter: TdPyCallable,
        wait_for_system_duration: chrono::Duration,
    ) -> (Self, ClockConfig) {
        (
            Self {
                dt_getter,
                wait_for_system_duration,
            },
            ClockConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable, chrono::Duration) {
        (
            "EventClockConfig",
            self.dt_getter.clone(),
            self.wait_for_system_duration,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (TdPyCallable, chrono::Duration) {
        (
            pyo3::Python::with_gil(TdPyCallable::pickle_new),
            chrono::Duration::zero(),
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("EventClockConfig", dt_getter, late)) = state.extract() {
            self.dt_getter = dt_getter;
            self.wait_for_system_duration = late;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for EventClockConfig: {state:?}"
            )))
        }
    }
}

pub(crate) struct EventClock {
    dt_getter: TdPyCallable,
    late: chrono::Duration,
    // State
    late_time: DateTime<Utc>,
    latest_event_time: Option<DateTime<Utc>>,
    system_time_of_last_event: DateTime<Utc>,
    watermark: DateTime<Utc>,
}

impl EventClock {
    fn new(
        dt_getter: TdPyCallable,
        late: chrono::Duration,
        latest_event_time: Option<DateTime<Utc>>,
        late_time: DateTime<Utc>,
        system_time_of_last_event: DateTime<Utc>,
        watermark: DateTime<Utc>,
    ) -> Self {
        Self {
            dt_getter,
            late,
            latest_event_time,
            late_time,
            system_time_of_last_event,
            watermark,
        }
    }
}

impl Clock<TdPyAny> for EventClock {
    fn watermark(&mut self, next_value: &Poll<Option<TdPyAny>>) -> DateTime<Utc> {
        let now = Utc::now();
        match next_value {
            Poll::Ready(Some(event)) => {
                let event_late_time = self.time_for(event) - self.late;
                if event_late_time > self.late_time {
                    self.late_time = event_late_time;
                    self.system_time_of_last_event = now;
                }
            }
            Poll::Ready(None) => {
                self.late_time = DateTime::<Utc>::MAX_UTC;
                self.system_time_of_last_event = now;
            }
            Poll::Pending => {}
        }
        let system_duration_since_last_event =
            now.signed_duration_since(self.system_time_of_last_event);
        // This is the watermark
        let watermark = self
            .late_time
            .checked_add_signed(system_duration_since_last_event)
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        watermark
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
        StateBytes::ser(&(
            self.latest_event_time,
            self.system_time_of_last_event,
            self.late_time,
            self.watermark,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_time_clock_serialization() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let dt_getter: TdPyCallable = TdPyCallable::pickle_new(py);
            let dt_getter_clone: TdPyCallable = dt_getter.clone_ref(py);
            let late = chrono::Duration::zero();
            let latest_event_time = Some(Utc::now());
            let late_time = Utc::now();
            let system_time_of_last_event = Utc::now();
            let watermark = Utc::now();
            let mut event_clock = EventClock::new(
                dt_getter,
                late,
                latest_event_time,
                late_time,
                system_time_of_last_event,
                watermark,
            );
            // Save the current watermark to check it doesn't change after
            // the roundtrip of serialization
            let watermark = event_clock.watermark(&Poll::Pending);

            // Take a snapshot
            let snapshot = event_clock.snapshot();

            // Rebuild an EventClock from the snapshot
            let config = EventClockConfig {
                dt_getter: dt_getter_clone,
                wait_for_system_duration: late,
            };
            let mut deserialized = config.builder()(Some(snapshot));
            let de_watermark = deserialized.watermark(&Poll::Pending);
            let now = Utc::now();

            // IF everything is (de)serialized correctly, the difference between
            // the previous watermark and the new one should be minimal, less than the
            // time it took to run everything since we instantiated the first watermark.
            // I (de)serialization didn't work, `late_time` would be initialized to
            // the minimum possible date, and time elapsed since then would be huge.
            assert!(
                watermark.signed_duration_since(de_watermark)
                    < watermark.signed_duration_since(now)
            );
        });
    }
}
