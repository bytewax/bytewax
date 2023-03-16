use std::panic::Location;

use pyo3::{PyErr, PyResult, PyTypeInfo, Python};

/// A trait that gives two method that can be used to return
/// a python exception building a stacktrace.
pub(crate) trait PythonException<T> {
    /// This trait can be implemented by anything
    /// that can be converted to a pyresult.
    fn into_pyresult(self) -> PyResult<T>;

    /// Make the existing exception part of the traceback and
    /// raise a custom exception with its own message.
    fn raise<PyErrType>(self, msg: &str) -> PyResult<T>
    where
        PyErrType: PyTypeInfo;

    /// Make the existing error part of the traceback and
    /// raise a new exception with the same type and a different message.
    fn reraise(self, msg: &str) -> PyResult<T>;

    fn _raise<PyErrType>(self, msg: &str, caller: &Location) -> PyResult<T>
    where
        PyErrType: PyTypeInfo,
        Self: Sized,
    {
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| PyErr::new::<PyErrType, _>(build_message(py, caller, &err, msg)))
        })
    }

    fn _reraise(self, msg: &str, caller: &Location) -> PyResult<T>
    where
        Self: Sized,
    {
        self.into_pyresult().map_err(|err| {
            Python::with_gil(|py| {
                PyErr::from_type(err.get_type(py), build_message(py, caller, &err, msg))
            })
        })
    }
}

impl<T> PythonException<T> for PyResult<T> {
    fn into_pyresult(self) -> PyResult<T> {
        self
    }

    #[track_caller]
    fn raise<PyErrType: PyTypeInfo>(self, msg: &str) -> PyResult<T> {
        let caller = Location::caller();
        self._raise::<PyErrType>(msg, caller)
    }

    #[track_caller]
    fn reraise(self, msg: &str) -> PyResult<T> {
        let caller = Location::caller();
        self._reraise(msg, caller)
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

fn prepend_caller(caller: &Location, msg: &str) -> String {
    format!("({caller}) {msg}")
}
