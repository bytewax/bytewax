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

use crate::errors::PythonException;
use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::try_unwrap;
use crate::unwrap_any;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

pub(crate) mod collect_window;
pub(crate) mod fold_window;
pub(crate) mod reduce;
pub(crate) mod reduce_window;
pub(crate) mod stateful_map;
pub(crate) mod stateful_unary;

#[tracing::instrument(level = "trace")]
pub(crate) fn map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyAny {
    Python::with_gil(|py| {
        unwrap_any!(mapper
            .call1(py, (item,))
            .reraise("error calling `map` mapper"))
        .into()
    })
}

#[tracing::instrument(level = "trace")]
pub(crate) fn flat_map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyIterator {
    Python::with_gil(|py| {
        try_unwrap!(mapper
            .call1(py, (item,))
            .reraise("error calling `flat_map` mapper")?
            .extract(py))
    })
}

#[tracing::instrument(level = "trace")]
pub(crate) fn filter(predicate: &TdPyCallable, item: &TdPyAny) -> bool {
    Python::with_gil(|py| {
        try_unwrap!({
            let should_emit_pybool: TdPyAny = predicate.call1(py, (item,))?.into();
            should_emit_pybool
                .extract(py)
                .raise::<PyTypeError>(&format!(
                    "return value of `predicate` in filter \
                operator must be a bool; got `{should_emit_pybool:?}` instead"
                ))
        })
    })
}

#[tracing::instrument(level = "trace")]
pub(crate) fn inspect(inspector: &TdPyCallable, item: &TdPyAny) {
    Python::with_gil(|py| {
        unwrap_any!(inspector
            .call1(py, (item,))
            .reraise("error calling `inspect` inspector"))
    });
}

#[tracing::instrument(level = "trace")]
pub(crate) fn inspect_epoch(inspector: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| {
        unwrap_any!(inspector
            .call1(py, (*epoch, item))
            .reraise("error calling `inspect_epoch` inspector"))
    });
}
