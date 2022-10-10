use std::task::Poll;

use chrono::{DateTime, Utc};
use pyo3::{exceptions::PyTypeError, prelude::*};
use tracing::field::debug;

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::{LogicFate, StateBytes, StatefulLogic},
    try_unwrap, unwrap_any,
};

/// Implements the reduce operator.
///
/// Combine values for a key into an accumulator, then after each
/// accumulator update, check if the accumulator should be emitted
/// downstream.
pub(crate) struct ReduceLogic {
    reducer: TdPyCallable,
    is_complete: TdPyCallable,
    acc: Option<TdPyAny>,
}

impl ReduceLogic {
    /// Returns a function that can deserialize the result of
    /// [`Self::snapshot`].
    pub(crate) fn builder(
        reducer: TdPyCallable,
        is_complete: TdPyCallable,
    ) -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_snapshot| {
            let acc = resume_snapshot.and_then(StateBytes::de::<Option<TdPyAny>>);
            Python::with_gil(|py| Self {
                reducer: reducer.clone_ref(py),
                is_complete: is_complete.clone_ref(py),
                acc,
            })
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for ReduceLogic {
    #[tracing::instrument(
        name = "reduce",
        level = "trace",
        skip(self),
        fields(self.reducer, self.is_complete, self.acc, updated_acc)
    )]
    fn on_awake(&mut self, next_value: Poll<Option<TdPyAny>>) -> Option<TdPyAny> {
        if let Poll::Ready(Some(value)) = next_value {
            tracing::trace!("Calling python reducer");
            Python::with_gil(|py| {
                let updated_acc: TdPyAny = match &self.acc {
                    // If there's no previous state for this key, use
                    // the current value.
                    None => value,
                    Some(acc) => unwrap_any!(self
                        .reducer
                        .call1(py, (acc.clone_ref(py), value.clone_ref(py))))
                    .into(),
                };
                tracing::Span::current().record("updated_acc", debug(&updated_acc));

                let should_emit_and_discard_acc: bool = try_unwrap!({
                    let should_emit_and_discard_acc_pybool: TdPyAny = self
                        .is_complete
                        .call1(py, (updated_acc.clone_ref(py),))?
                        .into();
                    should_emit_and_discard_acc_pybool
                        .extract(py)
                        .map_err(|_err| {
                            PyTypeError::new_err(format!(
                                "return value of `is_complete` in reduce operator must be a bool; \
                            got `{should_emit_and_discard_acc_pybool:?}` instead"
                            ))
                        })
                });

                tracing::Span::current().record(
                    "should_emit_and_discard_acc",
                    debug(&should_emit_and_discard_acc),
                );

                if should_emit_and_discard_acc {
                    self.acc = None;
                    Some(updated_acc)
                } else {
                    self.acc = Some(updated_acc);
                    None
                }
            })
        } else {
            None
        }
    }

    fn fate(&self) -> LogicFate {
        if self.acc.is_none() {
            LogicFate::Discard
        } else {
            LogicFate::Retain
        }
    }

    fn next_awake(&self) -> Option<DateTime<Utc>> {
        None
    }

    #[tracing::instrument(name = "reduce_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> StateBytes {
        StateBytes::ser::<Option<TdPyAny>>(&self.acc)
    }
}
