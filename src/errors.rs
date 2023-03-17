use std::panic::Location;

use pyo3::{exceptions::PyException, PyErr, PyResult, PyTypeInfo, Python};

/// A trait that gives two method that can be used to return
/// a python exception building a stacktrace.
pub(crate) trait PythonException<T> {
    /// This trait can be implemented by anything
    /// that can be converted to a pyresult.
    fn into_pyresult(self) -> PyResult<T>;

    /// Make the existing exception part of the traceback
    /// and raise a custom exception with its own message.
    #[track_caller]
    fn raise<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| PyErr::new::<PyErrType, _>(build_message(py, caller, &err, msg)))
        })
    }

    /// Make the existing error part of the traceback
    /// and raise a new exception with the same type
    /// and an additional message.
    #[track_caller]
    fn reraise(self, msg: &str) -> PyResult<T>
    where
        Self: Sized,
    {
        let caller = Location::caller();
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| {
                PyErr::from_type(err.get_type(py), build_message(py, caller, &err, msg))
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
        self.map_err(|err| PyErr::new::<PyException, _>(err.to_string()))
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

/// Prepend the name of the current thread to each line,
/// if present.
pub(crate) fn prepend_tname(msg: String) -> String {
    let tname = std::thread::current()
        .name()
        .unwrap_or("unnamed-thread")
        .to_string();
    msg.lines()
        .map(|line| format!("<{tname}> {line}\n"))
        .collect()
}
