use std::task::Poll;

use chrono::{Utc, DateTime};
use pyo3::{exceptions::PyValueError, pyclass, pymethods, PyAny, PyResult, Python, ToPyObject};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::StateBytes,
    window::ClockConfig,
};

use super::{
    system_clock::SystemClock,
    testing_clock::{TestingClock, TestingClockConfig},
    Builder, Clock, ClockBuilder,
};

/// Use datetimes from events as clock.
///
/// If the dataflow has no more input, all windows are closed.
///
/// The watermark is the system time since the last element
/// plus the value of `late`. It is updated every time an event
/// with a newer datetime is processed.
///
/// Args:
///   getter: Python function to get a datetime from an event.
///   late_after_system_duration: How much (system) time to wait before considering an event late.
///   system_clock_config: TODO
///
/// Returns:
///   Config object. Pass this as the `clock_config` parameter to your
///   windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(getter, late_after_system_duration, system_clock_config)")]
#[derive(Clone)]
pub(crate) struct EventClockConfig {
    #[pyo3(get)]
    pub(crate) dt_getter: TdPyCallable,
    #[pyo3(get)]
    pub(crate) late_after_system_duration: chrono::Duration,
    #[pyo3(get)]
    pub(crate) system_clock_config: Option<TestingClockConfig>,
}

impl ClockBuilder<TdPyAny> for EventClockConfig {
    fn builder(self) -> Builder<TdPyAny> {
        Box::new(move |resume_state_bytes: Option<StateBytes>| {
            let (latest_event_time, system_time_of_last_event, watermark) = resume_state_bytes
                .map(StateBytes::de)
                .unwrap_or_else(|| (None, None, Utc::now()));

            Box::new(EventClock::new(
                self.dt_getter.clone(),
                self.late_after_system_duration,
                system_time_of_last_event,
                latest_event_time,
                watermark,
                self.system_clock_config
                    .map(InternalClock::test)
                    .unwrap_or_else(InternalClock::system),
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
        late_after_system_duration: chrono::Duration,
        system_clock_config: Option<TestingClockConfig>,
    ) -> (Self, ClockConfig) {
        (
            Self {
                dt_getter,
                late_after_system_duration,
                system_clock_config,
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
        Option<TestingClockConfig>,
    ) {
        (
            "EventClockConfig",
            self.dt_getter.clone(),
            self.late_after_system_duration,
            self.system_clock_config,
        )
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (TdPyCallable, chrono::Duration, Option<TestingClockConfig>) {
        (
            pyo3::Python::with_gil(TdPyCallable::pickle_new),
            chrono::Duration::zero(),
            None,
        )
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("EventClockConfig", dt_getter, late, test)) = state.extract() {
            self.dt_getter = dt_getter;
            self.late_after_system_duration = late;
            self.system_clock_config = test;
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
    pub(crate) fn test(config: TestingClockConfig) -> Self {
        Self::Testing(TestingClock::new(config.item_incr, config.start_at))
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
}

pub(crate) struct EventClock {
    dt_getter: TdPyCallable,
    late: chrono::Duration,
    clock: InternalClock,
    // State
    latest_event_time: Option<DateTime<Utc>>,
    system_time_of_last_event: Option<DateTime<Utc>>,
    watermark: DateTime<Utc>,
}

impl EventClock {
    fn new(
        dt_getter: TdPyCallable,
        late: chrono::Duration,
        latest_event_time: Option<DateTime<Utc>>,
        system_time_of_last_event: Option<DateTime<Utc>>,
        watermark: DateTime<Utc>,
        clock: InternalClock,
    ) -> Self {
        Self {
            dt_getter,
            late,
            latest_event_time,
            system_time_of_last_event,
            watermark,
            clock,
        }
    }

    fn get_event_time(&self, event: &TdPyAny) -> DateTime<Utc> {
        Python::with_gil(|py| {
            self.dt_getter
                // Call the event time getter function with the event as parameter
                .call1(py, (event.to_object(py),))
                .unwrap()
                // Convert to DateTime<Utc>
                .extract(py)
                .unwrap()
        })
    }

    fn advance_watermark_by_event_time(&mut self, event_time: DateTime<Utc>) {
        // Get the latest_event_time:
        // max between self.latest_event_time and event_time
        // if self.latest_event_time is some, otherwise just event_time.
        let latest_event_time = self
            .latest_event_time
            .map(|latest_event_time| latest_event_time.max(event_time))
            .unwrap_or(event_time);
        // Assign it to self.latest_event_time
        self.latest_event_time = Some(latest_event_time);

        // Get current clock's time
        let now = self.clock.time();
        // Get watermark
        let watermark = now
            .checked_add_signed(now - latest_event_time)
            // Panic if the watermark overflowed
            .expect("watermark in date range (overflowed)")
            .checked_sub_signed(self.late)
            // Panic if the watermark underflowed
            .expect("watermark in date range (underflowed)");
        // But only assign it to self.watermark if it's after the current
        // one, since it can be advanced by system time too.
        self.watermark = watermark.max(self.watermark);

        // Also update self.system_time_of_last_event
        self.system_time_of_last_event = Some(now);
    }

    fn advance_watermark_by_system_time(&mut self) {
        let now = self.clock.time();
        let elapsed = now - self.system_time_of_last_event.unwrap_or(self.watermark);
        self.watermark = self
            .watermark
            .checked_add_signed(elapsed)
            .expect("watermark in date range (overflowed)");
    }
}

impl Clock<TdPyAny> for EventClock {
    fn watermark(&mut self, next_value: &Poll<Option<TdPyAny>>) -> DateTime<Utc> {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => DateTime::<Utc>::MAX_UTC,
            // If we received an event, set the new watermark and return it
            Poll::Ready(Some(event)) => {
                self.advance_watermark_by_event_time(self.get_event_time(event));
                self.watermark
            }
            // Advance the watermark by system time since
            // system_time_of_last_event, then return it
            _ => {
                self.advance_watermark_by_system_time();
                self.watermark
            }
        }
    }

    fn time_for(&mut self, event: &TdPyAny) -> DateTime<Utc> {
        self.get_event_time(event)
    }

    fn snapshot(&self) -> StateBytes {
        // Manually implement snapshotting here, because we'd have to
        // serialize the state two times if we used Clock::snapshot:
        // StateBytes::ser(&(
        //     self.latest_event_time,
        //     self.watermark,
        //     // This would already be serialized
        //     clock.snapshot()
        // ))
        match &self.clock {
            InternalClock::System(_) => StateBytes::ser(&(
                self.latest_event_time,
                self.system_time_of_last_event,
                self.watermark,
            )),
            InternalClock::Testing(clock) => StateBytes::ser(&(
                self.latest_event_time,
                self.system_time_of_last_event,
                self.watermark,
                clock.current_time(),
            )),
        }
    }
}
