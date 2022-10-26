#[macro_use(defer)]
extern crate scopeguard;

use pyo3::prelude::*;
use std::thread;
use std::time::Duration;

pub(crate) mod dataflow;
pub(crate) mod execution;
pub(crate) mod inputs;
pub(crate) mod operators;
pub(crate) mod outputs;
pub(crate) mod pyo3_extensions;
pub(crate) mod recovery;
pub(crate) mod tracing;
pub(crate) mod window;

#[macro_use]
pub(crate) mod macros;

/// Result type used in the crate that holds a String as the Err type.
pub(crate) type StringResult<T> = Result<T, String>;

#[pyfunction]
#[pyo3(text_signature = "(secs)")]
fn sleep_keep_gil(secs: u64) {
    thread::sleep(Duration::from_secs(secs));
}

#[pyfunction]
#[pyo3(text_signature = "(secs)")]
fn sleep_release_gil(py: Python, secs: u64) {
    py.allow_threads(|| {
        thread::sleep(Duration::from_secs(secs));
    });
}

/// Helper function used to setup tracing and logging from the Rust side.
///
/// Args:
///   tracing_config: A subclass of TracingConfig for a specific backend
///   log_level: String of the log level, on of ["ERROR", "WARN", "INFO", "DEBUG", "TRACE"]
///
/// By default it starts a tracer that logs all ERROR messages to stdout.
///
/// Note: to make this work, you have to keep a reference of the returned object:
///
/// ```python
/// tracer = setup_tracing()
/// ```
#[pyfunction]
#[pyo3(text_signature = "(tracing_config, log_level)")]
fn setup_tracing(
    py: Python,
    tracing_config: Option<Py<tracing::TracingConfig>>,
    log_level: Option<String>,
) -> crate::tracing::BytewaxTracer {
    let tracer = py.allow_threads(crate::tracing::BytewaxTracer::new);
    let config_tracer = tracing_config
        .map(|py_conf| crate::tracing::BytewaxTracer::extract_py_conf(py, py_conf).unwrap());
    py.allow_threads(|| {
        tracer.setup(config_tracer, log_level);
        tracer
    })
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_bytewax(py: Python, m: &PyModule) -> PyResult<()> {
    dataflow::register(py, m)?;
    execution::register(py, m)?;
    inputs::register(py, m)?;
    outputs::register(py, m)?;
    recovery::python::register(py, m)?;
    window::register(py, m)?;
    tracing::register(py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;
    m.add_function(wrap_pyfunction!(setup_tracing, m)?)?;

    Ok(())
}
