use log::debug;
use pyo3::prelude::*;

use crate::{
    pyo3_extensions::{TdPyAny, TdPyCallable},
    recovery::StateBytes,
    unwrap_any,
    window::WindowLogic,
};

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
        move |resume_snapshot| {
            let acc = resume_snapshot.and_then(StateBytes::de::<Option<TdPyAny>>);
            Python::with_gil(|py| Self {
                builder: builder.clone_ref(py),
                folder: folder.clone_ref(py),
                acc,
            })
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for FoldWindowLogic {
    fn with_next(&mut self, next_value: Option<TdPyAny>) -> Option<TdPyAny> {
        match next_value {
            Some(value) => Python::with_gil(|py| {
                let acc: TdPyAny = self
                    .acc
                    .take()
                    .unwrap_or_else(|| unwrap_any!(self.builder.call1(py, ())).into());
                // Call the folder with the initialized accumulator.
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
        StateBytes::ser::<Option<TdPyAny>>(&self.acc)
    }
}
