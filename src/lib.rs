use pyo3::prelude::*;

pub(crate) mod dataflow;
pub(crate) mod errors;
pub(crate) mod inputs;
pub(crate) mod metrics;
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

/// Internal Bytewax symbols from Rust.
///
/// These are re-imported elsewhere in the public `bytewax` module for
/// use.
#[pymodule]
#[pyo3(name = "_bytewax")]
fn mod_bytewax(py: Python, m: &PyModule) -> PyResult<()> {
    inputs::register(py, m)?;
    recovery::register(py, m)?;
    run::register(py, m)?;
    tracing::register(py, m)?;
    window::register(py, m)?;
    Ok(())
}
