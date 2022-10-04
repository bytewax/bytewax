use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyValueError, prelude::*};

use super::{Builder, ClockBuilder};
use super::{Clock, ClockConfig};
use crate::recovery::StateBytes;

/// Encapsulates and allows modifying the "now" when using
/// `bytewax.window.TestingClockConfig`. You only want to use this for
/// unit testing.
///
/// Args:
///
///   init_datetime (datetime.datetime): Initial "now".
///
/// Returns:
///
///   Testing clock object. Pass this as the `clock` parameter to
///   `bytewax.window.TestingClockConfig`.
#[pyclass(module = "bytewax.testing", name = "TestingClock")]
#[pyo3(text_signature = "(init_datetime)")]
#[derive(Clone)]
pub(crate) struct PyTestingClock {
    /// Modify this to change the current "now".
    #[pyo3(get, set)]
    pub(crate) now: DateTime<Utc>,
}

impl PyTestingClock {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(
            py,
            PyTestingClock {
                now: DateTime::<Utc>::MIN_UTC,
            },
        )
        .unwrap()
        .into()
    }
}

#[pymethods]
impl PyTestingClock {
    /// Tell pytest to ignore this class, even though it starts with
    /// the name "Test".
    #[allow(non_upper_case_globals)]
    #[classattr]
    const __test__: bool = false;

    #[new]
    #[args(init_datetime)]
    fn new(init_datetime: DateTime<Utc>) -> Self {
        Self { now: init_datetime }
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, DateTime<Utc>) {
        ("TestingClock", self.now)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (DateTime<Utc>,) {
        (DateTime::<Utc>::MIN_UTC,)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClock", now)) = state.extract() {
            self.now = now;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for TestingClock: {state:?}"
            )))
        }
    }
}

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
///   clock (TestingClock): Query this `TestingClock` for the current
///       "now".
///
/// Returns:
///
///   Config object. Pass this as the `clock_config` parameter to
///   your windowing operator.
#[pyclass(module="bytewax.window", extends=ClockConfig)]
#[pyo3(text_signature = "(clock)")]
#[derive(Clone)]
pub(crate) struct TestingClockConfig {
    #[pyo3(get)]
    pub(crate) clock: Py<PyTestingClock>,
}

impl<V> ClockBuilder<V> for TestingClockConfig {
    fn builder(self) -> Builder<V> {
        // Do not restore state here since we don't store anything.
        // See note in the `snapshot` method implementation for TestingClock.
        Box::new(move |_resume_snapshot: Option<StateBytes>| {
            // All instances of this [`TestingClock`] will reference
            // the same [`PyTestingClock`] so modifications increment
            // all windows' times.
            Box::new(TestingClock::new(self.clock.clone()))
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
    #[args(item_incr, start_at)]
    fn new(clock: Py<PyTestingClock>) -> (Self, ClockConfig) {
        (Self { clock }, ClockConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self, py: Python) -> (&str, Py<PyTestingClock>) {
        ("TestingClockConfig", self.clock.clone_ref(py))
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (Py<PyTestingClock>,) {
        (PyTestingClock::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("TestingClockConfig", clock)) = state.extract() {
            self.clock = clock;
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
    py_clock: Py<PyTestingClock>,
}

impl TestingClock {
    pub(crate) fn new(py_clock: Py<PyTestingClock>) -> Self {
        Self { py_clock }
    }
}

impl<V> Clock<V> for TestingClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> DateTime<Utc> {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => DateTime::<Utc>::MAX_UTC,
            _ => Python::with_gil(|py| {
                let py_clock = self.py_clock.borrow(py);
                py_clock.now
            }),
        }
    }

    fn time_for(&mut self, item: &V) -> DateTime<Utc> {
        self.watermark(&Poll::Ready(Some(item)))
    }

    /// Does not store state since state is per-key but the testing
    /// clock is referencing a single global "system time".
    ///
    /// Instead you should re-perform your modifications to the
    /// [`PyTestingClock`] on the Python side as part of input
    /// recovery.
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<()>(&())
    }
}
