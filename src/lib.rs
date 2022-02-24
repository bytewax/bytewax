#[macro_use(defer)]
extern crate scopeguard;

use pyo3::prelude::*;
use std::thread;
use std::time::Duration;

use timely::dataflow::*;

pub(crate) mod dataflow;
pub(crate) mod execution;
pub(crate) mod operators;
pub(crate) mod webserver;
pub(crate) mod pyo3_extensions;

#[macro_use]
pub(crate) mod macros;

use pyo3_extensions::{TdPyIterator, TdPyAny};

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

/// Encapsulates the process of pulling data out of the input Python
/// iterator and feeding it into Timely.
///
/// This will be called in the worker's "main" loop to feed data in.
pub(crate) struct Pump {
    pull_from_pyiter: TdPyIterator,
    pyiter_is_empty: bool,
    push_to_timely: InputHandle<u64, TdPyAny>,
}

impl Pump {
    fn new(pull_from_pyiter: TdPyIterator, push_to_timely: InputHandle<u64, TdPyAny>) -> Self {
        Self {
            pull_from_pyiter,
            pyiter_is_empty: false,
            push_to_timely,
        }
    }

    /// Take a single data element and timestamp and feed it into the
    /// dataflow.
    fn pump(&mut self) {
        Python::with_gil(|py| {
            let mut pull_from_pyiter = self.pull_from_pyiter.0.as_ref(py);
            if let Some(epoch_item_pytuple) = pull_from_pyiter.next() {
                let (epoch, item) = with_traceback!(py, epoch_item_pytuple?.extract());
                self.push_to_timely.advance_to(epoch);
                self.push_to_timely.send(item);
            } else {
                self.pyiter_is_empty = true;
            }
        });
    }

    fn input_remains(&self) -> bool {
        !self.pyiter_is_empty
    }
}

#[pymodule]
#[pyo3(name = "bytewax")]
fn mod_tiny_dancer(_py: Python, m: &PyModule) -> PyResult<()> {
    execution::register(_py, m)?;
    dataflow::register(_py, m)?;

    m.add_function(wrap_pyfunction!(sleep_keep_gil, m)?)?;
    m.add_function(wrap_pyfunction!(sleep_release_gil, m)?)?;

    Ok(())
}
