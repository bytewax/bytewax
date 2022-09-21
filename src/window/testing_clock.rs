use std::task::Poll;

use chrono::NaiveDateTime;
use pyo3::{exceptions::PyValueError, prelude::*};

use super::{Clock, ClockConfig};
use crate::recovery::StateBytes;

/// Encapsulates and allows modifyingt the "now" when using
/// `bytewax.window.TestingClockConfig`.
///
/// Args:
///
///     init_datetime (datetime.datetime): Initial "now".
///
/// Returns:
///
///     Testing clock object. Pass this as the `clock` parameter to
///     `bytewax.window.TestingClockConfig`.
#[pyclass(module = "bytewax.testing", name = "TestingClock")]
#[pyo3(text_signature = "(init_datetime)")]
pub(crate) struct PyTestingClock {
    /// Modify this to change the current "now".
    #[pyo3(get, set)]
    now: pyo3_chrono::NaiveDateTime,
}

impl PyTestingClock {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(
            py,
            PyTestingClock {
                now: pyo3_chrono::NaiveDateTime(chrono::naive::MIN_DATETIME),
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
    fn new(init_datetime: pyo3_chrono::NaiveDateTime) -> Self {
        Self { now: init_datetime }
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, pyo3_chrono::NaiveDateTime) {
        ("TestingClock", self.now)
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self) -> (pyo3_chrono::NaiveDateTime,) {
        (pyo3_chrono::NaiveDateTime(chrono::naive::MIN_DATETIME),)
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

/// Use to simulate system time in tests.
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
pub(crate) struct TestingClockConfig {
    #[pyo3(get)]
    pub(crate) clock: Py<PyTestingClock>,
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
    pub(crate) fn builder<V>(
        py_clock: Py<PyTestingClock>,
    ) -> impl Fn(Option<StateBytes>) -> Box<dyn Clock<V>> {
        move |resume_state_bytes: Option<StateBytes>| {
            // All instances of this [`TestingClock`] will reference
            // the same [`PyTestingClock`] so modifications increment
            // all windows' times.
            let py_clock = py_clock.clone();

            if let Some(now) = resume_state_bytes.map(StateBytes::de::<NaiveDateTime>) {
                Python::with_gil(|py| {
                    py_clock.borrow_mut(py).now = pyo3_chrono::NaiveDateTime(now);
                })
            }

            Box::new(Self { py_clock })
        }
    }
}

impl<V> Clock<V> for TestingClock {
    fn watermark(&mut self, next_value: &Poll<Option<V>>) -> NaiveDateTime {
        match next_value {
            // If there will be no more values, close out all windows.
            Poll::Ready(None) => chrono::naive::MAX_DATETIME,
            _ => Python::with_gil(|py| {
                let py_clock = self.py_clock.borrow(py);
                py_clock.now.0.clone()
            }),
        }
    }

    fn time_for(&mut self, item: &V) -> NaiveDateTime {
        self.watermark(&Poll::Ready(Some(item)))
    }

    fn snapshot(&self) -> StateBytes {
        let now = Python::with_gil(|py| self.py_clock.borrow(py).now.0);
        StateBytes::ser::<NaiveDateTime>(&now)
    }
}
