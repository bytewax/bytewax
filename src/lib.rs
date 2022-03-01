#[macro_use(defer)]
extern crate scopeguard;

use pyo3::prelude::*;
use std::thread;
use std::time::Duration;

pub(crate) mod dataflow;
pub(crate) mod execution;
pub(crate) mod operators;
pub(crate) mod pyo3_extensions;
pub(crate) mod webserver;

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
fn mod_tiny_dancer(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();

    execution::register(_py, m)?;
    dataflow::register(_py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
