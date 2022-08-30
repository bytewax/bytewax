use std::{task::Poll, time::Duration};

use log::debug;
use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::{StateBytes, StateUpdate, StatefulLogic},
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
        move |resume_acc_bytes| {
            let acc = resume_acc_bytes.map(|resume_acc_bytes| resume_acc_bytes.de());
            Python::with_gil(|py| Self {
                reducer: reducer.clone_ref(py),
                is_complete: is_complete.clone_ref(py),
                acc,
            })
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for ReduceLogic {
    fn exec(&mut self, next_value: Poll<Option<TdPyAny>>) -> (Option<TdPyAny>, Option<Duration>) {
        if let Poll::Ready(Some(value)) = next_value {
            Python::with_gil(|py| {
                let updated_acc: TdPyAny = match &self.acc {
                    // If there's no previous state for this key, use
                    // the current value.
                    None => value,
                    Some(acc) => {
                        let updated_acc = unwrap_any!(self
                            .reducer
                            .call1(py, (acc.clone_ref(py), value.clone_ref(py))))
                        .into();
                        debug!(
                            "reduce: reducer={:?}(acc={acc:?}, value={value:?}) \
                            -> updated_acc={updated_acc:?}",
                            self.reducer
                        );
                        updated_acc
                    }
                };

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
                debug!(
                    "reduce: is_complete={:?}(updated_acc={updated_acc:?}) \
                    -> should_emit_and_discard_acc={should_emit_and_discard_acc:?}",
                    self.is_complete
                );

                if should_emit_and_discard_acc {
                    self.acc = None;
                    (Some(updated_acc), None)
                } else {
                    self.acc = Some(updated_acc);
                    (None, None)
                }
            })
        } else {
            (None, None)
        }
    }

    fn snapshot(&self) -> StateUpdate {
        match &self.acc {
            Some(acc) => StateUpdate::Upsert(StateBytes::ser(acc)),
            None => StateUpdate::Reset,
        }
    }
}
