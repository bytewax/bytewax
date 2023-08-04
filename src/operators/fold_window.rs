use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use tracing::field::debug;

use crate::{
    errors::PythonException,
    pyo3_extensions::{TdPyAny, TdPyCallable},
    unwrap_any,
    window::*,
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
    ) -> impl Fn(Option<TdPyAny>) -> Self {
        move |resume_snapshot| {
            Python::with_gil(|py| {
                let acc = resume_snapshot
                    .and_then(|state| {
                        let state: Option<Option<TdPyAny>> = unwrap_any!(state.extract(py));
                        state
                    })
                    .unwrap_or_default();

                Self {
                    builder: builder.clone_ref(py),
                    folder: folder.clone_ref(py),
                    acc,
                }
            })
        }
    }
}

impl WindowLogic<TdPyAny, TdPyAny, Option<TdPyAny>> for FoldWindowLogic {
    #[tracing::instrument(
        name = "fold_window",
        level = "trace",
        skip(self),
        fields(self.builder, self.folder, self.acc, updated_acc),
    )]
    fn with_next(&mut self, next_value: Option<(TdPyAny, DateTime<Utc>)>) -> Option<TdPyAny> {
        match next_value {
            Some((value, _item_time)) => Python::with_gil(|py| {
                let acc: TdPyAny = self.acc.take().unwrap_or_else(|| {
                    unwrap_any!(self
                        .builder
                        .call1(py, ())
                        .reraise("error calling FoldWindow builder"))
                    .into()
                });
                // Call the folder with the initialized accumulator.
                let updated_acc = unwrap_any!(self
                    .folder
                    .call1(py, (acc.clone_ref(py), value.clone_ref(py)))
                    .reraise("error calling FoldWindow folder"))
                .into();
                tracing::Span::current().record("updated_acc", debug(&updated_acc));
                self.acc = Some(updated_acc);
                None
            }),
            // Emit at end of window.
            None => self.acc.take(),
        }
    }

    #[tracing::instrument(name = "fold_window_snapshot", level = "trace", skip_all)]
    fn snapshot(&self) -> TdPyAny {
        Python::with_gil(|py| self.acc.to_object(py).into())
    }
}
