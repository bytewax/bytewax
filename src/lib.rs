use pyo3::prelude::*;
use pyo3_extensions::PyConfigClass;
use std::thread;
use std::time::Duration;

pub(crate) mod common;
pub(crate) mod dataflow;
pub(crate) mod errors;
pub(crate) mod inputs;
pub(crate) mod operators;
pub(crate) mod outputs;
pub(crate) mod pyo3_extensions;
pub(crate) mod recovery;
pub(crate) mod run;
pub(crate) mod timely;
pub(crate) mod tracing;
pub(crate) mod webserver;
pub(crate) mod window;
pub(crate) mod worker;

#[macro_use]
pub(crate) mod macros;

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
) -> PyResult<crate::tracing::BytewaxTracer> {
    let tracer = py.allow_threads(crate::tracing::BytewaxTracer::new);
    let builder = tracing_config.map(|conf| conf.downcast(py).unwrap());
    py.allow_threads(|| tracer.setup(builder, log_level))?;
    Ok(tracer)
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_bytewax(py: Python, m: &PyModule) -> PyResult<()> {
    dataflow::register(py, m)?;
    run::register(py, m)?;
    recovery::python::register(py, m)?;
    window::register(py, m)?;
    tracing::register(py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;
    m.add_function(wrap_pyfunction!(setup_tracing, m)?)?;

    Ok(())
}
