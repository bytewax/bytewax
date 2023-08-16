use pyo3::prelude::*;
use std::thread;
use std::time::Duration;

pub(crate) mod dataflow;
pub(crate) mod errors;
pub(crate) mod inputs;
pub(crate) mod operators;
pub(crate) mod outputs;
pub(crate) mod pyo3_extensions;
pub(crate) mod recovery;
pub(crate) mod run;
pub(crate) mod serde;
pub(crate) mod timely;
pub(crate) mod tracing;
pub(crate) mod webserver;
pub(crate) mod window;
pub(crate) mod worker;

#[macro_use]
pub(crate) mod macros;

#[pyfunction]
fn sleep_keep_gil(secs: u64) {
    thread::sleep(Duration::from_secs(secs));
}

#[pyfunction]
fn sleep_release_gil(py: Python, secs: u64) {
    py.allow_threads(|| {
        thread::sleep(Duration::from_secs(secs));
    });
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_bytewax(py: Python, m: &PyModule) -> PyResult<()> {
    dataflow::register(py, m)?;
    run::register(py, m)?;
    recovery::register(py, m)?;
    window::register(py, m)?;
    tracing::register(py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
