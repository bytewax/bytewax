use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyValueError, prelude::*};

use super::{Builder, ClockBuilder};
use super::{Clock, ClockConfig};
use crate::pyo3_extensions::{TdPyAny, TdPyCallable};
use crate::recovery::StateBytes;

/// Use to simulate system time in unit tests. You only want to use
/// this for unit testing.
///
/// You should use this with
/// `bytewax.inputs.TestingBuilderInputConfig` and a generator which
/// modifies the `TestingClock` provided.
///
/// If the dataflow has no more input, all windows are closed.
///
/// The watermark uses the most recent "now".
///
/// Args:
///
///  dt_getter: function used to retrieve a UTC datetime from each event.
///
///  wait_until_end: wether to wait for all data to be collected, or
///                  not wait at all for late events.
///
/// Returns:
///
///   Config object. Pass this as the `clock_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(dt_getter, wait_until_end)")]
#[derive(Clone)]
pub(crate) struct TestingClockConfig {
    #[pyo3(get)]
    pub(crate) dt_getter: TdPyCallable,
    #[pyo3(get)]
    pub(crate) wait_until_end: bool
}

impl ClockBuilder<TdPyAny> for TestingClockConfig {
    fn builder(self) -> Builder<TdPyAny> {
        Box::new(move |resume_snapshot: Option<StateBytes>| {
            let system_time_of_last_event = resume_snapshot
                .map(StateBytes::de)
                .unwrap_or_else(Utc::now);
            Box::new(TestingClock::new(
                self.dt_getter.clone(),
                system_time_of_last_event,
                self.wait_until_end,
            ))
        })
    }
}

#[pymethods]
impl TestingClockConfig {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args(dt_getter)]
    fn new(dt_getter: TdPyCallable, wait_until_end: bool) -> (Self, ClockConfig) {
        (Self { dt_getter, wait_until_end }, ClockConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self, _py: Python) -> (&str, TdPyCallable, bool) {
        ("TestingClockConfig", self.dt_getter.clone(), self.wait_until_end)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, _py: Python) -> (TdPyCallable, bool) {
        (pyo3::Python::with_gil(TdPyCallable::pickle_new), true)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClockConfig", dt_getter, wait_until_end)) = state.extract() {
            self.dt_getter = dt_getter;
            self.wait_until_end = wait_until_end;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TestingClockConfig: {state:?}"
            )))
        }
    }
}

/// Simulate system time for tests. Call upon [`PyTestingClock`] for
/// the current time.
pub(crate) struct TestingClock {
    dt_getter: TdPyCallable,
    wait_until_end: bool,
    // State
    system_time_of_last_event: DateTime<Utc>,
    late_time: DateTime<Utc>,
}

impl TestingClock {
    pub(crate) fn new(dt_getter: TdPyCallable, system_time_of_last_event: DateTime<Utc>, wait_until_end: bool) -> Self {
        Self {
            dt_getter,
            wait_until_end,
            system_time_of_last_event,
            late_time: DateTime::<Utc>::MIN_UTC,
        }
    }
}

impl Clock<TdPyAny> for TestingClock {
    fn watermark(&mut self, next_value: &Poll<Option<TdPyAny>>) -> DateTime<Utc> {
        let now = Utc::now();
        // If we want to wait, set the watermark to ~1000 years before,
        // so that hopefully no event is ever considered late.
        let late = if self.wait_until_end {
            chrono::Duration::weeks(52_000)
        } else {
            chrono::Duration::zero()
        };
        match next_value {
            Poll::Ready(Some(event)) => {
                let event_late_time = self.time_for(event) - late;
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
        self.late_time
            .checked_add_signed(system_duration_since_last_event)
            .unwrap_or(DateTime::<Utc>::MAX_UTC)
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

    /// Does not store state since state is per-key but the testing
    /// clock is referencing a single global "system time".
    ///
    /// Instead you should re-perform your modifications to the
    /// [`PyTestingClock`] on the Python side as part of input
    /// recovery.
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<DateTime<Utc>>(&self.system_time_of_last_event)
    }
}
