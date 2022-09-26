use std::task::Poll;

use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyCoroIterator};
use crate::recovery::StateBytes;
use crate::{py_unwrap, try_unwrap};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

use super::{InputConfig, InputReader};

/// Construct a Python iterator for each worker from a builder
/// function.
pub(crate) struct ManualInput {
    pyiter: TdPyCoroIterator,
    last_state: TdPyAny,
}

impl ManualInput {
    pub(crate) fn new(
        py: Python,
        input_builder: TdPyCallable,
        worker_index: usize,
        worker_count: usize,
        resume_state_bytes: Option<StateBytes>,
    ) -> Self {
        let resume_state: TdPyAny = resume_state_bytes
            .map(StateBytes::de::<TdPyAny>)
            .unwrap_or_else(|| py.None().into());

        let pyiter: TdPyCoroIterator = try_unwrap!(input_builder
            .call1(py, (worker_index, worker_count, resume_state.clone_ref(py)))?
            .extract(py));

        Self {
            pyiter,
            last_state: resume_state,
        }
    }
}

impl InputReader<TdPyAny> for ManualInput {
    fn next(&mut self) -> Poll<Option<TdPyAny>> {
        self.pyiter.next().map(|poll| {
            poll.map(|state_item_pytuple| {
                Python::with_gil(|py| {
                    let (updated_state, item): (TdPyAny, TdPyAny) = py_unwrap!(
                        state_item_pytuple.extract(py),
                        format!(
                            "Manual input builders must yield `(state, item)` \
                                two-tuples; got `{state_item_pytuple:?}` instead"
                        )
                    );

                    self.last_state = updated_state;

                    item
                })
            })
        })
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<TdPyAny>(&self.last_state)
    }
}

/// Use a user-defined function that returns an iterable as the input
/// source.
///
/// Because Bytewax's execution is cooperative, the resulting
/// iterators _must not block_ waiting for new data, otherwise pending
/// execution of other steps in the dataflow will be delayed an
/// throughput will be reduced. If you are using a generator and no
/// data is ready yet, have it `yield None` or just `yield` to signal
/// this.
///
/// Args:
///
///   input_builder: `input_builder(worker_index: int, worker_count:
///       int, resume_state: Option[Any]) => Iterator[Tuple[Any,
///       Any]]` Builder function which returns an iterator of
///       2-tuples of `(state, item)`. `item` is the input that
///       worker should introduce into the dataflow. `state` is a
///       snapshot of any internal state it will take to resume this
///       input from its current position _after the current
///       item_. Note that e.g. returning the same list from each
///       worker will result in duplicate data in the dataflow.
///
/// Returns:
///
///   Config object. Pass this as the `input_config` argument to the
///   `bytewax.dataflow.Dataflow.input`.
#[pyclass(module = "bytewax.inputs", extends = InputConfig)]
#[pyo3(text_signature = "(input_builder)", subclass)]
pub(crate) struct ManualInputConfig {
    #[pyo3(get)]
    pub(crate) input_builder: TdPyCallable,
}

#[pymethods]
impl ManualInputConfig {
    #[new]
    #[args(input_builder)]
    pub(crate) fn new(input_builder: TdPyCallable) -> (Self, InputConfig) {
        (Self { input_builder }, InputConfig {})
    }

    /// Pickle as a tuple.
    fn __getstate__(&self) -> (&str, TdPyCallable) {
        ("ManualInputConfig", self.input_builder.clone())
    }

    /// Egregious hack see [`SqliteRecoveryConfig::__getnewargs__`].
    fn __getnewargs__(&self, py: Python) -> (TdPyCallable,) {
        (TdPyCallable::pickle_new(py),)
    }

    /// Unpickle from tuple of arguments.
    fn __setstate__(&mut self, state: &PyAny) -> PyResult<()> {
        if let Ok(("ManualInputConfig", input_builder)) = state.extract() {
            self.input_builder = input_builder;
            Ok(())
        } else {
            Err(PyValueError::new_err(format!(
                "bad pickle contents for ManualInputConfig: {state:?}"
            )))
        }
    }
}
