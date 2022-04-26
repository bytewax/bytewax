/// These are all shims which map the Timely Rust API into equivalent
/// calls to Python functions through PyO3.
use crate::recovery::StepRecovery;
use std::collections::HashMap;
use std::hash::Hash;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::Data;
use timely::ExchangeData;

use pyo3::prelude::*;

use crate::log_func;
use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::with_traceback;
use log::debug;
use std::thread;

pub(crate) fn map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyAny {
    debug!("{}, mapper:{:?}, item:{:?}", log_func!(), mapper, item);
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))).into())
}

pub(crate) fn flat_map(mapper: &TdPyCallable, item: TdPyAny) -> TdPyIterator {
    debug!("{}, mapper:{:?}, item:{:?}", log_func!(), mapper, item);
    Python::with_gil(|py| with_traceback!(py, mapper.call1(py, (item,))?.extract(py)))
}

pub(crate) fn filter(predicate: &TdPyCallable, item: &TdPyAny) -> bool {
    debug!(
        "{}, predicate:{:?}, item:{:?}",
        log_func!(),
        predicate,
        item
    );
    Python::with_gil(|py| with_traceback!(py, predicate.call1(py, (item,))?.extract(py)))
}

pub(crate) fn inspect(inspector: &TdPyCallable, item: &TdPyAny) {
    debug!(
        "{}, inspector:{:?}, item:{:?}",
        log_func!(),
        inspector,
        item
    );
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (item,)));
    });
}

pub(crate) fn inspect_epoch(inspector: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    debug!(
        "{}, inspector:{:?}, item:{:?}",
        log_func!(),
        inspector,
        item
    );
    Python::with_gil(|py| {
        with_traceback!(py, inspector.call1(py, (*epoch, item)));
    });
}

// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait Reduce<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> {
    fn reduce<
        R: Fn(&K, V, V) -> V + 'static,  // Reducer
        C: Fn(&K, &V) -> bool + 'static, // Is reduction complete and send aggregator downstream?
        H: Fn(&K) -> u64 + 'static,      // Hash function of key to worker
    >(
        &self,
        store: Box<dyn StepRecovery<S::Timestamp, K, V>>,
        reducer: R,
        is_complete: C,
        hasher: H,
    ) -> Stream<S, (K, V)>
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> Reduce<S, K, V> for Stream<S, (K, V)> {
    fn reduce<
        R: Fn(&K, V, V) -> V + 'static,  // Reducer
        C: Fn(&K, &V) -> bool + 'static, // Is reduction complete and send aggregator downstream?
        H: Fn(&K) -> u64 + 'static,      // Hash function of key to worker
    >(
        &self,
        store: Box<dyn StepRecovery<S::Timestamp, K, V>>,
        reducer: R,
        is_complete: C,
        hasher: H,
    ) -> Stream<S, (K, V)> {
        let mut tmp_key_values: Vec<(K, V)> = Vec::new();

        let mut epoch_to_key_values_buffer: HashMap<S::Timestamp, Vec<(K, V)>> = HashMap::new();
        // A value will not exist in the hash map if we need to go to
        // the recovery store to check for a value, but will exist and
        // be None if it has been reset by logic code.
        let mut state_cache: HashMap<K, Option<V>> = HashMap::new();
        // Fill this with which keys have had their state updated and
        // flush to recovery store each epoch. Will be emptied after
        // each epoch.
        let mut updated_keys_buffer = Vec::new();

        self.unary_notify(
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
            "Reduce",
            vec![],
            move |input, output, ncater| {
                input.for_each(|time, key_values| {
                    key_values.swap(&mut tmp_key_values);

                    epoch_to_key_values_buffer
                        .entry(time.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_key_values.drain(..));

                    ncater.notify_at(time.retain());
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                ncater.for_each(|time, _, _| {
                    let timestamp = time.time();
                    if let Some(key_values) = epoch_to_key_values_buffer.remove(timestamp) {
                        let mut output_session = output.session(&time);
                        // TODO: Fetch all new keys in one DB round
                        // trip?
                        for (key, value) in key_values {
                            let aggregator_for_key = state_cache
                                .remove(&key)
                                // If we've never seen this key before
                                // (not even in the state cache),
                                .unwrap_or_else(|| {
                                    // Find what the last aggregator
                                    // for this key was in the
                                    // recovery store, which might be
                                    // explicitly (the logic threw it
                                    // away by returning None) or
                                    // implicitly (a new key we've
                                    // never seen) None.
                                    store.recover_last(time.time(), &key)
                                });

                            let updated_aggregator_for_key =
                                if let Some(aggregator_for_key) = aggregator_for_key {
                                    // If there's previous aggregator,
                                    // reduce.
                                    reducer(&key, aggregator_for_key, value)
                                } else {
                                    // If there's no previous aggregator,
                                    // pass through the current value.
                                    value
                                };

                            // Note which keys have had their state
                            // modified. Write that updated state to
                            // the recovery store once the epoch is
                            // complete.
                            updated_keys_buffer.push(key.clone());
                            // Save the aggregator so we can use it if
                            // multiple values within this epoch. We
                            // do not save to the recovery store here
                            // because we don't have finalized state
                            // for the epoch.
                            if is_complete(&key, &updated_aggregator_for_key) {
                                output_session.give((key.clone(), updated_aggregator_for_key));
                                state_cache.insert(key, None);
                            } else {
                                state_cache.insert(key, Some(updated_aggregator_for_key));
                            }
                        }

                        // Now that this epoch is complete, write out
                        // any modified state.
                        for key in updated_keys_buffer.drain(..) {
                            if let Some(updated_aggregator_for_key) = state_cache.get(&key) {
                                // TODO: Save all updated keys in one DB
                                // round trip?
                                store.save_complete(time.time(), &key, updated_aggregator_for_key);
                            }
                        }
                    }
                });
            },
        )
    }
}

pub(crate) fn reduce(
    reducer: &TdPyCallable,
    key: &TdPyAny,
    aggregator: TdPyAny,
    value: TdPyAny,
) -> TdPyAny {
    Python::with_gil(|py| {
        let updated_aggregator: TdPyAny = with_traceback!(
            py,
            reducer.call1(py, (aggregator.clone_ref(py), value.clone_ref(py)))
        )
        .into();
        debug!("reduce for key={key:?}: reducer={reducer:?}(aggregator={aggregator:?}, value={value:?}) -> updated_aggregator={updated_aggregator:?}",);
        updated_aggregator
    })
}

pub(crate) fn check_complete(
    is_complete: &TdPyCallable,
    key: &TdPyAny,
    aggregator: &TdPyAny,
) -> bool {
    Python::with_gil(|py| {
        let should_emit_and_discard_aggregator: bool = with_traceback!(
            py,
            is_complete
                .call1(py, (aggregator.clone_ref(py),))?
                .extract(py)
        );
        debug!("reduce for key={key:?}: is_complete={is_complete:?}(updated_aggregator={aggregator:?}) -> should_emit_and_discard_aggregator={should_emit_and_discard_aggregator:?}");
        should_emit_and_discard_aggregator
    })
}

pub(crate) fn reduce_epoch(
    reducer: &TdPyCallable,
    aggregator: &mut Option<TdPyAny>,
    _key: &TdPyAny,
    value: TdPyAny,
) {
    debug!(
        "{}, reducer:{:?}, key:{:?}, value:{:?}, aggregator:{:?}",
        log_func!(),
        reducer,
        _key,
        value,
        aggregator
    );
    Python::with_gil(|py| {
        let updated_aggregator = match aggregator {
            Some(aggregator) => {
                with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value))).into()
            }
            None => value,
        };
        *aggregator = Some(updated_aggregator);
        debug!(
            "{}, reducer:{:?}, key:{:?}, updated_aggregator:{:?}",
            log_func!(),
            reducer,
            _key,
            aggregator
        );
    });
}

pub(crate) fn reduce_epoch_local(
    reducer: &TdPyCallable,
    aggregators: &mut HashMap<TdPyAny, TdPyAny>,
    all_key_value_in_epoch: &Vec<(TdPyAny, TdPyAny)>,
) {
    Python::with_gil(|py| {
        let _current = thread::current();
        let id = _current.id();
        for (key, value) in all_key_value_in_epoch {
            let aggregator = aggregators.entry(key.clone_ref(py));
            debug!(
                "thread:{:?}, {}, reducer:{:?}, key:{:?}, value:{:?}, aggregator:{:?}",
                id,
                log_func!(),
                reducer,
                key,
                value,
                aggregator
            );
            aggregator
                .and_modify(|aggregator| {
                    *aggregator =
                        with_traceback!(py, reducer.call1(py, (aggregator.clone_ref(py), value)))
                            .into();
                    debug!(
                        "{}, reducer:{:?}, key:{:?}, value:{:?}, updated_aggregator:{:?}",
                        log_func!(),
                        reducer,
                        key,
                        value,
                        *aggregator
                    );
                })
                .or_insert(value.clone_ref(py));
        }
    });
}

// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulMap<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> {
    fn stateful_map<
        R: Data,                                     // Output item type
        D: 'static,                                  // Per-key state
        I: IntoIterator<Item = R>,                   // Iterator of output items
        B: Fn(&K) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&K, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        store: Box<dyn StepRecovery<S::Timestamp, K, D>>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> Stream<S, (K, R)>
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> StatefulMap<S, K, V>
    for Stream<S, (K, V)>
{
    fn stateful_map<
        R: Data,                                     // Output item type
        D: 'static,                                  // Per-key state
        I: IntoIterator<Item = R>,                   // Iterator of output items
        B: Fn(&K) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&K, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        store: Box<dyn StepRecovery<S::Timestamp, K, D>>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> Stream<S, (K, R)> {
        let mut tmp_key_values: Vec<(K, V)> = Vec::new();

        let mut epoch_to_key_values_buffer: HashMap<S::Timestamp, Vec<(K, V)>> = HashMap::new();
        // A value will not exist in the hash map if we need to go to
        // the recovery store to check for a value, but will exist and
        // be None if it has been reset by logic code.
        let mut state_cache: HashMap<K, Option<D>> = HashMap::new();
        // Fill this with which keys have had their state updated and
        // flush to recovery store each epoch. Will be emptied after
        // each epoch.
        let mut updated_keys_buffer = Vec::new();

        self.unary_notify(
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
            "StatefulMap",
            vec![],
            move |input, output, ncater| {
                input.for_each(|time, key_values| {
                    key_values.swap(&mut tmp_key_values);

                    epoch_to_key_values_buffer
                        .entry(time.time().clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_key_values.drain(..));

                    ncater.notify_at(time.retain());
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                ncater.for_each(|time, _, _| {
                    let timestamp = time.time();
                    if let Some(key_values) = epoch_to_key_values_buffer.remove(timestamp) {
                        let mut output_session = output.session(&time);
                        // TODO: Fetch all new keys in one DB round
                        // trip?
                        for (key, value) in key_values {
                            let state_for_key = state_cache
                                .remove(&key)
                                // If we've never seen this key before
                                // (not even in the state cache),
                                .unwrap_or_else(|| {
                                    // Find what the last state for
                                    // this key was in the recovery
                                    // store, which might be
                                    // explicitly (the logic threw it
                                    // away by returning None) or
                                    // implicitly (a new key we've
                                    // never seen) None.
                                    store.recover_last(time.time(), &key)
                                })
                                // If there's no previous state, build
                                // anew.
                                .unwrap_or_else(|| builder(&key));

                            let (updated_state_for_key, output) =
                                mapper(&key, state_for_key, value);

                            // Note which keys have had their state
                            // modified. Write that updated state to
                            // the recovery store once the epoch is
                            // complete.
                            updated_keys_buffer.push(key.clone());
                            output_session
                                .give_iterator(output.into_iter().map(|item| (key.clone(), item)));
                            // Save the state so we can use it if
                            // multiple values within this epoch. We
                            // do not save to the recovery store here
                            // because we don't have finalized state
                            // for the epoch.
                            state_cache.insert(key, updated_state_for_key);
                        }

                        // Now that this epoch is complete, write out
                        // any modified state.
                        for key in updated_keys_buffer.drain(..) {
                            if let Some(updated_state_for_key) = state_cache.get(&key) {
                                // TODO: Save all updated keys in one DB
                                // round trip?
                                store.save_complete(time.time(), &key, updated_state_for_key);
                            }
                        }
                    }
                });
            },
        )
    }
}

pub(crate) fn build(builder: &TdPyCallable, key: &TdPyAny) -> TdPyAny {
    Python::with_gil(|py| {
        let initial_state = with_traceback!(py, builder.call1(py, (key.clone_ref(py),))).into();
        debug!("build for key={key:?}: builder={builder:?}(key={key:?}) -> initial_state{initial_state:?}");
        initial_state
    })
}

pub(crate) fn stateful_map(
    mapper: &TdPyCallable,
    key: &TdPyAny,
    state: TdPyAny,
    value: TdPyAny,
) -> (Option<TdPyAny>, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let (updated_state, emit_value): (Option<TdPyAny>, TdPyAny) = with_traceback!(
            py,
            mapper
                .call1(py, (state.clone_ref(py), value.clone_ref(py)))?
                .extract(py)
        );
        debug!("stateful_map for key={key:?}: mapper={mapper:?}(state={state:?}, value={value:?}) -> (updated_state={updated_state:?}, emit_value={emit_value:?})");

        (updated_state, std::iter::once(emit_value))
    })
}

pub(crate) fn capture(captor: &TdPyCallable, epoch: &u64, item: &TdPyAny) {
    Python::with_gil(|py| with_traceback!(py, captor.call1(py, ((*epoch, item.clone_ref(py)),))));
}
