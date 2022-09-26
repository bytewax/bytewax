use std::{task::Poll, time::Duration};

use log::debug;
use pyo3::{exceptions::PyTypeError, prelude::*};

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::{StateBytes, StateUpdate, StatefulLogic},
    try_unwrap, unwrap_any,
};

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
                    .map(StateBytes::de::<TdPyAny>)
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
                let (updated_state, updated_value): (Option<TdPyAny>, TdPyAny) = try_unwrap!({
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
                });
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
            Some(state) => StateUpdate::Upsert(StateBytes::ser::<TdPyAny>(state)),
            None => StateUpdate::Reset,
        }
    }
}
