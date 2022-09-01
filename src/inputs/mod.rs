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

use crate::pyo3_extensions::TdPyAny;
use crate::recovery::StateBytes;
use crate::StringResult;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rdkafka::Offset;
use send_wrapper::SendWrapper;
use std::task::Poll;

pub(crate) mod kafka_input;
pub(crate) mod manual_input;

pub(crate) use self::kafka_input::{KafkaInput, KafkaInputConfig};
pub(crate) use self::manual_input::{ManualInput, ManualInputConfig};

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

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str,) {
        ("InputConfig",)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("InputConfig",)) = state.extract() {
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for InputConfig: {state:?}"
            )))
        }
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

// TODO: Convert this to use the builder pattern to match the other
// stateful components.
pub(crate) fn build_input_reader(
    py: Python,
    config: Py<InputConfig>,
    worker_index: usize,
    worker_count: usize,
    resume_state_bytes: Option<StateBytes>,
) -> StringResult<Box<dyn InputReader<TdPyAny>>> {
    // See comment in [`crate::recovery::build_recovery_writers`]
    // about releasing the GIL during IO class building.
    let config = config.as_ref(py);

    if let Ok(config) = config.downcast::<PyCell<ManualInputConfig>>() {
        let config = config.borrow();

        let input_builder = config.input_builder.clone();

        // This one can't release the GIL because we're calling Python
        // to construct it.
        let reader = ManualInput::new(
            py,
            input_builder,
            worker_index,
            worker_count,
            resume_state_bytes,
        );

        Ok(Box::new(reader))
    } else if let Ok(config) = config.downcast::<PyCell<KafkaInputConfig>>() {
        let config = config.borrow();

        let brokers = &config.brokers;
        let topic = &config.topic;
        let tail = config.tail;
        let starting_offset = match config.starting_offset.as_str() {
            "beginning" => Ok(Offset::Beginning),
            "end" => Ok(Offset::End),
            unk => Err(format!(
                "starting_offset should be either `\"beginning\"` or `\"end\"`; got `{unk:?}`"
            )),
        }?;
        let additional_configs = &config.additional_configs;

        let reader = py.allow_threads(|| {
            SendWrapper::new(KafkaInput::new(
                brokers,
                topic,
                tail,
                starting_offset,
                additional_configs,
                worker_index,
                worker_count,
                resume_state_bytes,
            ))
        });

        Ok(Box::new(reader.take()))
    } else {
        let pytype = config.get_type();
        Err(format!("Unknown input_config type: {pytype}"))
    }
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
