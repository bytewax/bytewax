use log::debug;
use pyo3::prelude::*;

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
    fn with_next(&mut self, next_value: Option<TdPyAny>) -> Option<TdPyAny> {
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
        StateBytes::ser::<Option<TdPyAny>>(&self.acc)
    }
}
