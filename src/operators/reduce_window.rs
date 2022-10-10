use pyo3::prelude::*;
use tracing::field::debug;

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::StateBytes,
    unwrap_any,
    window::WindowLogic,
};

/// Implements the reduce window operator.
///
/// Combine values within a window into an accumulator. Emit the
/// accumulator when the window closes.
pub(crate) struct ReduceWindowLogic {
    reducer: TdPyCallable,
    acc: Option<TdPyAny>,
}

impl ReduceWindowLogic {
    pub(crate) fn builder(reducer: TdPyCallable) -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_snapshot| {
            let acc = resume_snapshot.and_then(StateBytes::de::<Option<TdPyAny>>);
            Python::with_gil(|py| Self {
                reducer: reducer.clone_ref(py),
                acc,
            })
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for ReduceWindowLogic {
    #[tracing::instrument(
        name = "reduce_window",
        level = "trace",
        skip(self),
        fields(self.reducer, self.acc, updated_acc)
    )]
    fn with_next(&mut self, next_value: Option<TdPyAny>) -> Option<TdPyAny> {
        match next_value {
            Some(value) => {
                Python::with_gil(|py| {
                    let updated_acc: TdPyAny = match &self.acc {
                        // If there's no previous state for this key,
                        // use the current value.
                        None => value,
                        Some(acc) => {
                            tracing::trace!("Calling python reducer");
                            unwrap_any!(self.reducer.call1(py, (acc, value))).into()
                        }
                    };
                    tracing::Span::current().record("updated_acc", debug(&updated_acc));
                    self.acc = Some(updated_acc);
                    None
                })
            }
            // Emit at end of window.
            None => self.acc.take(),
        }
    }

    #[tracing::instrument(name = "reduce_window_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<Option<TdPyAny>>(&self.acc)
    }
}
