use std::panic::Location;

use pyo3::exceptions::PyException;
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::PyTracebackMethods;
use pyo3::PyDowncastError;
use pyo3::PyErr;
use pyo3::PyResult;
use pyo3::PyTypeInfo;
use pyo3::Python;

import_exception!(bytewax.errors, BytewaxRuntimeError);

/// A trait to build a python exception with a custom stacktrace from
/// anything that can be converted into a PyResult.
pub(crate) trait PythonException<T> {
    /// Only this needs to be implemented.
    fn into_pyresult(self) -> PyResult<T>;

    /// Make the existing exception part of the traceback and raise a
    /// custom exception with its own message.
    ///
    /// Example:
    ///
    /// ```ignore
    /// func().raise::<PyTypeError>("Raise TypeError adding this message")?;
    /// ```
    #[track_caller]
    fn raise<PyErrType: PyTypeInfo>(self, msg: &'static str) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| PyErr::new::<PyErrType, _>(build_message(py, caller, &err, msg)))
        })
    }

    /// Make the existing exception part of the traceback and raise a
    /// custom exception with its own message.
    ///
    /// Example:
    ///
    /// ```ignore
    /// func().raise_with::<PyTypeError>(|| format("Raise TypeError adding this message"))?;
    /// ```
    #[track_caller]
    fn raise_with<PyErrType: PyTypeInfo>(self, f: impl FnOnce() -> String) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            let msg = f();
            Python::with_gil(|py| PyErr::new::<PyErrType, _>(build_message(py, caller, &err, &msg)))
        })
    }

    /// Create a new BytewaxRuntimeError with a custom message, setting
    /// the current exception as it's cause.
    ///
    /// Example:
    ///
    /// ```ignore
    /// func().reraise("Reraise a new BytewaxRuntimeError with this message")?;
    /// ```
    #[track_caller]
    fn reraise(self, msg: &'static str) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| {
                let new_err = BytewaxRuntimeError::new_err(format!("({caller}): {msg}"));
                new_err.set_cause(py, Some(err));
                new_err
            })
        })
    }

    /// Create a new BytewaxRuntimeError with a custom message, setting
    /// the current exception as it's cause.
    ///
    /// Example:
    ///
    /// ```ignore
    /// func().reraise_with(|| format("Reraise a RuntimeError with this message"))?;
    /// ```
    #[track_caller]
    fn reraise_with(self, f: impl FnOnce() -> String) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            let msg = f();
            Python::with_gil(|py| {
                let new_err = BytewaxRuntimeError::new_err(format!("({caller}): {msg}"));
                new_err.set_cause(py, Some(err));
                new_err
            })
        })
    }
}

// The obvious implementation for PyResult
impl<T> PythonException<T> for PyResult<T> {
    fn into_pyresult(self) -> PyResult<T> {
        self
    }
}

// Some useful implementations for other kind of errors
impl<T> PythonException<T> for Result<T, tracing::subscriber::SetGlobalDefaultError> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyException, _>(err.to_string()))
    }
}

impl<T> PythonException<T> for std::io::Result<T> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyException, _>(err.to_string()))
    }
}

impl<T> PythonException<T> for Result<T, opentelemetry::trace::TraceError> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyException, _>(err.to_string()))
    }
}

impl<T> PythonException<T> for Result<T, String> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(PyErr::new::<PyException, _>)
    }
}

impl<T> PythonException<T> for Result<T, Box<dyn std::error::Error>> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyException, _>(format!("{err}")))
    }
}

impl<T> PythonException<T> for Result<T, PyDowncastError<'_>> {
    fn into_pyresult(self) -> PyResult<T> {
        self.map_err(|err| PyErr::new::<PyException, _>(format!("{err}")))
    }
}

/// Use this function to create a PyErr with location tracking.
#[track_caller]
pub(crate) fn tracked_err<PyErrType: PyTypeInfo>(msg: &str) -> PyErr {
    let caller = Location::caller();
    PyErr::new::<PyErrType, _>(prepend_caller(caller, msg))
}

fn build_message(py: Python, caller: &Location, err: &PyErr, msg: &str) -> String {
    let msg = prepend_caller(caller, msg);

    let err_msg = get_traceback(py, err)
        .map(|tb| format!("{err}\n{tb}"))
        .unwrap_or_else(|| format!("{err}"));

    format!("{msg}\nCaused by => {err_msg}")
}

fn get_traceback(py: Python, err: &PyErr) -> Option<String> {
    err.traceback(py).map(|tb| {
        tb.format()
            .unwrap_or_else(|_| "Unable to print traceback".to_string())
    })
}

/// Prepend '({caller}) ' to the message
fn prepend_caller(caller: &Location, msg: &str) -> String {
    format!("({caller}) {msg}")
}
