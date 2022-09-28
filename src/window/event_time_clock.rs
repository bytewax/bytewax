use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, Py, PyAny, PyResult, Python};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::StateBytes,
    window::ClockConfig,
};

use super::{
    system_clock::SystemClock,
    testing_clock::{PyTestingClock, TestingClock},
    Builder, Clock, ClockBuilder,
};

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
///   system_clock: Optional configuration to use a PyTestingClock as the internal system_clock,
///                 only used for testing purposes right now.
///
/// Returns:
///   Config object. Pass this as the `clock_config` parameter to your
///   windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(dt_getter, wait_for_system_duration, system_clock)")]
#[derive(Clone)]
pub(crate) struct EventClockConfig {
    #[pyo3(get)]
    pub(crate) dt_getter: TdPyCallable,
    #[pyo3(get)]
    pub(crate) wait_for_system_duration: chrono::Duration,
    #[pyo3(get)]
    pub(crate) system_clock: Option<Py<PyTestingClock>>,
}

impl ClockBuilder<TdPyAny> for EventClockConfig {
    fn builder(self) -> Builder<TdPyAny> {
        Box::new(move |resume_state_bytes: Option<StateBytes>| {
            // Make the clock an InternalClock now, so we can retrieve
            // it's `time` to use as the default value.
            let mut clock = self
                .system_clock
                .to_owned()
                .map(InternalClock::test)
                .unwrap_or_else(InternalClock::system);

            // Deserialize data if a snapshot existed
            let (latest_event_time, system_time_of_last_event, late_time, watermark) =
                resume_state_bytes.map(StateBytes::de).unwrap_or_else(|| {
                    // Defaults values
                    (
                        None,
                        // system_time_of_last_event
                        clock.time(),
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
                clock,
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
        system_clock: Option<Py<PyTestingClock>>,
    ) -> (Self, ClockConfig) {
        (
            Self {
                dt_getter,
                wait_for_system_duration,
                system_clock,
            },
            ClockConfig {},
        )
    }

    /// Pickle as a tuple.
    fn __getstate__(
        &self,
    ) -> (
        &str,
        TdPyCallable,
        chrono::Duration,
        Option<Py<PyTestingClock>>,
    ) {
        (
            "EventClockConfig",
            self.dt_getter.clone(),
            self.wait_for_system_duration,
            self.system_clock.clone(),
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (TdPyCallable, chrono::Duration, Option<Py<PyTestingClock>>) {
        (
            pyo3::Python::with_gil(TdPyCallable::pickle_new),
            chrono::Duration::zero(),
            None,
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("EventClockConfig", dt_getter, late, test_clock)) = state.extract() {
            self.dt_getter = dt_getter;
            self.wait_for_system_duration = late;
            self.system_clock = test_clock;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for EventClockConfig: {state:?}"
            )))
        }
    }
}

enum InternalClock {
    Testing(TestingClock),
    System(SystemClock),
}

impl InternalClock {
    pub(crate) fn test(py_clock: Py<PyTestingClock>) -> Self {
        Self::Testing(TestingClock::new(py_clock))
    }

    pub(crate) fn system() -> Self {
        Self::System(SystemClock::new())
    }

    pub(crate) fn time(&mut self) -> DateTime<Utc> {
        // Since this should only be SystemClock or TestingClock, the value
        // we pass to `time_for` is not used, and we can just pass ().
        match self {
            Self::Testing(clock) => clock.time_for(&()),
            Self::System(clock) => clock.time_for(&()),
        }
    }

    pub(crate) fn snapshot<V>(&self) -> StateBytes {
        match self {
            Self::Testing(clock) => <TestingClock as Clock<V>>::snapshot(clock),
            Self::System(clock) => <SystemClock as Clock<V>>::snapshot(clock),
        }
    }
}

pub(crate) struct EventClock {
    dt_getter: TdPyCallable,
    late: chrono::Duration,
    system_clock: InternalClock,
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
        clock: InternalClock,
    ) -> Self {
        Self {
            dt_getter,
            late,
            latest_event_time,
            late_time,
            system_time_of_last_event,
            watermark,
            system_clock: clock,
        }
    }
}

impl Clock<TdPyAny> for EventClock {
    fn watermark(&mut self, next_value: &Poll<Option<TdPyAny>>) -> DateTime<Utc> {
        let now = self.system_clock.time();
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
            self.system_clock.snapshot::<TdPyAny>(),
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
            let py_clock_now = Utc::now();
            let py_clock = Py::new(py, PyTestingClock { now: py_clock_now }).unwrap();
            let py_clock_clone = py_clock.clone_ref(py);
            let clock = InternalClock::test(py_clock);
            let mut event_clock = EventClock::new(
                dt_getter,
                late,
                latest_event_time,
                late_time,
                system_time_of_last_event,
                watermark,
                clock,
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
                system_clock: Some(py_clock_clone),
            };
            let mut deserialized = config.builder()(Some(snapshot));

            // If everything was correctly serialized/deserialized, the
            // watermark shouldn't have changed:
            assert_eq!(deserialized.watermark(&Poll::Pending), watermark);
        });
    }
}
