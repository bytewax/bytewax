use std::collections::HashMap;

use pyo3::prelude::*;

use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::with_traceback;

// These are all shims which map the Timely Rust API into equivalent
// calls to Python functions through PyO3.

pub(crate) fn map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyAny {
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))).into())
}

pub(crate) fn flat_map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyIterator {
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))?.extract(py)))
}

pub(crate) fn filter(predicate: &TdPyCallable, item: &TdPyAny) -> bool {
    Python::with_gil(|py| with_traceback!(py, predicate.call1(py, (item,))?.extract(py)))
}

pub(crate) fn inspect(inspector: &TdPyCallable, item: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (item,)));
    });
}

pub(crate) fn inspect_epoch(inspector: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (*epoch, item)));
    });
}

pub(crate) fn reduce(
    reducer: &TdPyCallable,
    is_complete: &TdPyCallable,
    aggregator: &mut Option<TdPyAny>,
    key: &TdPyAny,
    value: TdPyAny,
) -> (bool, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let updated_aggregator = match aggregator {
            Some(aggregator) => {
                with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value))).into()
            }
            None => value,
        };
        let should_emit_and_discard_aggregator: bool = with_traceback!(
            py,
            is_complete
                .call1(py, (updated_aggregator.clone_ref(py),))?
                .extract(py)
        );

        *aggregator = Some(updated_aggregator.clone_ref(py));

        if should_emit_and_discard_aggregator {
            let emit = (key.clone_ref(py), updated_aggregator).to_object(py).into();
            (true, Some(emit))
        } else {
            (false, None)
        }
    })
}

pub(crate) fn reduce_epoch(
    reducer: &TdPyCallable,
    aggregator: &mut Option<TdPyAny>,
    _key: &TdPyAny,
    value: TdPyAny,
) {
    Python::with_gil(|py| {
        let updated_aggregator = match aggregator {
            Some(aggregator) => {
                with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value))).into()
            }
            None => value,
        };
        *aggregator = Some(updated_aggregator);
    });
}

pub(crate) fn reduce_epoch_local(
    reducer: &TdPyCallable,
    aggregators: &mut HashMap<TdPyAny, TdPyAny>,
    all_key_value_in_epoch: &Vec<(TdPyAny, TdPyAny)>,
) {
    Python::with_gil(|py| {
        for (key, value) in all_key_value_in_epoch {
            aggregators
                .entry(key.clone_ref(py))
                .and_modify(|aggregator| {
                    *aggregator =
                        with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value)))
                            .into()
                })
                .or_insert(value.clone_ref(py));
        }
    });
}

pub(crate) fn stateful_map(
    mapper: &TdPyCallable,
    state: &mut TdPyAny,
    key: &TdPyAny,
    value: TdPyAny,
) -> (bool, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let (updated_state, emit_value): (TdPyAny, TdPyAny) = with_traceback!(
            py,
            mapper.call1(py, (state.clone_ref(py), value))?.extract(py)
        );

        *state = updated_state;

        let discard_state = Python::with_gil(|py| state.is_none(py));
        let emit = (key, emit_value).to_object(py).into();
        (discard_state, std::iter::once(emit))
    })
}

pub(crate) fn capture(captor: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| with_traceback!(py, captor.call1(py, ((*epoch, item.clone_ref(py)),))));
}
