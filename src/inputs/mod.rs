//! Internal code for input systems.
//!
//! For a user-centric version of input, read the `bytewax.input`
//! Python module docstring. Read that first.
//!
//! Architecture
//! ------------
//!
//! The one extra quirk here is that input is completely decoupled
//! from epoch generation. See [`crate::execution`] for how Timely
//! sources are generated and epochs assigned. The only goal of the
//! input system is "what's my next item for this worker?"
//!
//! Input is based around the core trait of [`InputReader`].  The
//! [`crate::dataflow::Dataflow::input`] operator delegates to impls
//! of that trait for actual writing.
//!
//! This system follows our standard pattern of having parallel Python
//! config objects and Rust impl structs for each trait of behavior we
//! want. E.g. [`KafkaInputConfig`] represents a token in Python for
//! how to create a [`KafkaInput`].

use crate::common::StringResult;
use crate::execution::{WorkerCount, WorkerIndex};
use crate::pyo3_extensions::{PyConfigClass, TdPyAny};
use crate::recovery::model::StateBytes;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::task::Poll;

pub(crate) mod kafka_input;
pub(crate) mod manual_input;

pub(crate) use self::kafka_input::KafkaInputConfig;
pub(crate) use self::manual_input::ManualInputConfig;

/// Base class for an input config.
///
/// These define how you will input data to your dataflow.
///
/// Use a specific subclass of InputConfig for the kind of input
/// source you are plan to use. See the subclasses in this module.
#[pyclass(module = "bytewax.inputs", subclass)]
#[pyo3(text_signature = "()")]
pub(crate) struct InputConfig;

impl InputConfig {
    /// Create an "empty" [`Self`] just for use in `__getnewargs__`.
    #[allow(dead_code)]
    pub(crate) fn pickle_new(py: Python) -> Py<Self> {
        PyCell::new(py, InputConfig {}).unwrap().into()
    }
}

#[pymethods]
impl InputConfig {
    #[new]
    fn new() -> Self {
        Self {}
    }

    /// Return a representation of this class as a PyDict.
    fn __getstate__(&self) -> HashMap<&str, Py<PyAny>> {
        Python::with_gil(|py| HashMap::from([("type", "InputConfig".into_py(py))]))
    }

    /// Unpickle from a PyDict.
    fn __setstate__(&mut self, _state: &PyAny) -> PyResult<()> {
        Ok(())
    }
}

// This trait has to be implemented by each InputConfig subclass.
// It is used to create an InputReader from an InputConfig.
pub(crate) trait InputBuilder {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_snapshot: Option<StateBytes>,
    ) -> StringResult<Box<dyn InputReader<TdPyAny>>>;
}

// Extract a specific InputConfig from a parent InputConfig coming from python.
impl PyConfigClass<Box<dyn InputBuilder>> for Py<InputConfig> {
    fn downcast(&self, py: Python) -> StringResult<Box<dyn InputBuilder>> {
        if let Ok(conf) = self.extract::<ManualInputConfig>(py) {
            Ok(Box::new(conf))
        } else if let Ok(conf) = self.extract::<KafkaInputConfig>(py) {
            Ok(Box::new(conf))
        } else {
            let pytype = self.as_ref(py).get_type();
            Err(format!("Unknown input_config type: {pytype}"))
        }
    }
}

impl InputBuilder for Py<InputConfig> {
    fn build(
        &self,
        py: Python,
        worker_index: WorkerIndex,
        worker_count: WorkerCount,
        resume_snapshot: Option<StateBytes>,
    ) -> StringResult<Box<dyn InputReader<TdPyAny>>> {
        self.downcast(py)?
            .build(py, worker_index, worker_count, resume_snapshot)
    }
}

/// Defines how a single source of input reads data.
pub(crate) trait InputReader<D> {
    /// Return the next item from this input, if any.
    ///
    /// This method must _never block or wait_ on data. If there's no
    /// data yet, return [`Poll::Pending`].
    ///
    /// This has the same semantics as
    /// [`std::async_iter::AsyncIterator::poll_next`]:
    ///
    /// - [`Poll::Pending`]: no new values ready yet.
    ///
    /// - [`Poll::Ready`] with a [`Some`]: a new value has arrived.
    ///
    /// - [`Poll::Ready`] with a [`None`]: the stream has ended and
    ///   [`next`] should not be called again.
    fn next(&mut self) -> Poll<Option<D>>;

    /// Snapshot the internal state of this input.
    ///
    /// Serialize any and all state necessary to re-construct this
    /// input exactly how it is currently. This will probably mean
    /// writing out any offsets in the various input streams.
    fn snapshot(&self) -> StateBytes;
}

// TODO: One day could use this impl for the Python version in
// `bytewax.inputs.distribute`? Hard to do because there's no PyO3
// bridging of `impl Iterator`.
fn distribute<T>(
    it: impl IntoIterator<Item = T>,
    index: usize,
    count: usize,
) -> impl Iterator<Item = T> {
    assert!(index < count);
    it.into_iter()
        .enumerate()
        .filter(move |(i, _x)| i % count == index)
        .map(|(_i, x)| x)
}

#[test]
fn test_distribute() {
    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 0, 2).collect();
    let expected = vec!["blue", "red"];
    assert_eq!(found, expected);

    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 1, 2).collect();
    let expected = vec!["green"];
    assert_eq!(found, expected);
}

#[test]
fn test_distribute_empty() {
    let found: Vec<_> = distribute(vec!["blue", "green", "red"], 3, 5).collect();
    let expected: Vec<&str> = vec![];
    assert_eq!(found, expected);
}

pub(crate) fn register(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<InputConfig>()?;
    m.add_class::<ManualInputConfig>()?;
    m.add_class::<KafkaInputConfig>()?;
    Ok(())
}
