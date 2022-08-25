//! Code implementing Bytewax's operators.
//!
//! The two big things in here are:
//!
//!   1. Shim functions that totally encapsulate PyO3 and Python
//!   calling boilerplate to easily pass into Timely operators.
//!
//!   2. Implementation of stateful operators using
//!   [`crate::recovery::StatefulLogic`] and
//!   [`crate::window::WindowLogic`].

use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::recovery::StateBytes;
use crate::recovery::StateUpdate;
use crate::recovery::StatefulLogic;
use crate::window::WindowLogic;
use crate::try_unwrap;
use crate::{log_func, unwrap_any};
use log::debug;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::task::Poll;
use std::time::Duration;

pub(crate) fn map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyAny {
    debug!("{}, mapper:{:?}, item:{:?}", log_func!(), mapper, item);
    Python::with_gil(|py| unwrap_any!(mapper.call1(py, (item,))).into())
}

pub(crate) fn flat_map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyIterator {
    debug!("{}, mapper:{:?}, item:{:?}", log_func!(), mapper, item);
    Python::with_gil(|py| try_unwrap!(mapper.call1(py, (item,))?.extract(py)))
}

pub(crate) fn filter(predicate: &TdPyCallable, item: &TdPyAny) -> bool {
    debug!(
        "{}, predicate:{:?}, item:{:?}",
        log_func!(),
        predicate,
        item
    );
    Python::with_gil(|py| {
        try_unwrap!({
            let should_emit_pybool: TdPyAny = predicate.call1(py, (item,))?.into();
            should_emit_pybool.extract(py).map_err(|_err| {
                PyTypeError::new_err(format!(
                    "return value of `predicate` in filter \
                operator must be a bool; got `{should_emit_pybool:?}` instead"
                ))
            })
        })
    })
}

pub(crate) fn inspect(inspector: &TdPyCallable, item: &TdPyAny) {
    debug!(
        "{}, inspector:{:?}, item:{:?}",
        log_func!(),
        inspector,
        item
    );
    Python::with_gil(|py| unwrap_any!(inspector.call1(py, (item,))));
}

pub(crate) fn inspect_epoch(inspector: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    debug!(
        "{}, inspector:{:?}, item:{:?}",
        log_func!(),
        inspector,
        item
    );
    Python::with_gil(|py| unwrap_any!(inspector.call1(py, (*epoch, item))));
}

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

/// Implements the stateful map operator.
///
/// Map incoming values, having access to a persistent shared state
/// for each key.
pub(crate) struct StatefulMapLogic {
    builder: TdPyCallable,
    mapper: TdPyCallable,
    state: Option<TdPyAny>,
}

impl StatefulMapLogic {
    /// Returns a function that can deserialize the result of
    /// [`Self::snapshot`].
    pub(crate) fn builder(
        builder: TdPyCallable,
        mapper: TdPyCallable,
    ) -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_state_bytes| {
            let state = Some(
                resume_state_bytes
                    .map(|resume_state_bytes| resume_state_bytes.de())
                    .unwrap_or_else(|| {
                        Python::with_gil(|py| {
                            let initial_state: TdPyAny = unwrap_any!(builder.call1(py, ())).into();
                            debug!(
                                "stateful_map: builder={:?}() -> initial_state{initial_state:?}",
                                builder
                            );
                            initial_state
                        })
                    }),
            );

            Python::with_gil(|py| Self {
                builder: builder.clone_ref(py),
                mapper: mapper.clone_ref(py),
                state,
            })
        }
    }
}

impl StatefulLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for StatefulMapLogic {
    fn exec(&mut self, next_value: Poll<Option<TdPyAny>>) -> (Option<TdPyAny>, Option<Duration>) {
        if let Poll::Ready(Some(value)) = next_value {
            Python::with_gil(|py| {
                let state = self.state.get_or_insert_with(|| {
                    let initial_state: TdPyAny = unwrap_any!(self.builder.call1(py, ())).into();
                    debug!(
                        "stateful_map: builder={:?}() -> initial_state{initial_state:?}",
                        self.builder
                    );
                    initial_state
                });
                let (updated_state, updated_value): (Option<TdPyAny>, TdPyAny) = try_unwrap!(
                    {
                        let updated_state_value_pytuple: TdPyAny = self
                            .mapper
                            .call1(py, (state.clone_ref(py), value.clone_ref(py)))?
                            .into();
                        updated_state_value_pytuple
                            .extract(py)
                            .map_err(|_err|
                                PyTypeError::new_err(
                                    format!("return value of `mapper` in stateful map operator must be a 2-tuple of `(updated_state, updated_value)`; \
                                        got `{updated_state_value_pytuple:?}` instead")
                                )
                            )
                    }
                );
                debug!(
                    "stateful_map: mapper={:?}(state={:?}, value={value:?}) -> \
                    (updated_state={updated_state:?}, updated_value={updated_value:?})",
                    self.mapper, self.state
                );

                self.state = updated_state;

                (Some(updated_value), None)
            })
        } else {
            (None, None)
        }
    }

    fn snapshot(&self) -> StateUpdate {
        match &self.state {
            Some(state) => StateUpdate::Upsert(StateBytes::ser(state)),
            None => StateUpdate::Reset,
        }
    }
}

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
        move |resume_acc_bytes| {
            let acc = resume_acc_bytes.map(|resume_acc_bytes| resume_acc_bytes.de());
            Python::with_gil(|py| Self {
                reducer: reducer.clone_ref(py),
                acc,
            })
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for ReduceWindowLogic {
    fn exec(&mut self, next_value: Option<TdPyAny>) -> Option<TdPyAny> {
        match next_value {
            Some(value) => {
                Python::with_gil(|py| {
                    let updated_acc: TdPyAny = match &self.acc {
                        // If there's no previous state for this key,
                        // use the current value.
                        None => value,
                        Some(acc) => {
                            let updated_acc = unwrap_any!(self
                                .reducer
                                .call1(py, (acc.clone_ref(py), value.clone_ref(py))))
                            .into();
                            debug!("reduce_window: reducer={:?}(acc={acc:?}, value={value:?}) -> updated_acc={updated_acc:?}", self.reducer);

                            updated_acc
                        }
                    };

                    self.acc = Some(updated_acc);

                    None
                })
            }
            // Emit at end of window.
            None => self.acc.take(),
        }
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.acc)
    }
}

/// Implements the fold window operator.
///
/// Combine values within a window into an accumulator built by the builder function.
/// Emit the accumulator when the window closes.
pub(crate) struct FoldWindowLogic {
    builder: TdPyCallable,
    folder: TdPyCallable,
    acc: Option<TdPyAny>,
}

impl FoldWindowLogic {
    pub(crate) fn new(
        builder: TdPyCallable,
        folder: TdPyCallable,
    ) -> impl Fn(Option<StateBytes>) -> Self {
        move |resume_acc_bytes| {
            let acc = resume_acc_bytes.map(|resume_acc_bytes| resume_acc_bytes.de());
            Python::with_gil(|py| Self {
                builder: builder.clone_ref(py),
                folder: folder.clone_ref(py),
                acc,
            })
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for FoldWindowLogic {
    fn exec(&mut self, next_value: Option<TdPyAny>) -> Option<TdPyAny> {
        match next_value {
            Some(value) => Python::with_gil(|py| {
                let acc: TdPyAny = self
                    .acc
                    .take()
                    .unwrap_or_else(|| unwrap_any!(self.builder.call1(py, ())).into());
                // Call the folder with the initialized accumulator
                let updated_acc = unwrap_any!(self
                    .folder
                    .call1(py, (acc.clone_ref(py), value.clone_ref(py))))
                .into();
                debug!(
                    "fold_window: builder={:?}, folder={:?}(acc={acc:?}, value={value:?}) \
                        -> updated_acc={updated_acc:?}",
                    self.builder, self.folder
                );
                self.acc = Some(updated_acc);
                None
            }),
            // Emit at end of window.
            None => self.acc.take(),
        }
    }

    fn snapshot(&self) -> StateBytes {
        StateBytes::ser(&self.acc)
    }
}
