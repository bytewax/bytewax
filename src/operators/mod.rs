//! Code implementing Bytewax's operators on top of Timely's
//! operators.
//!
//! The two big things in here are:
//!
//!     1. Totally custom Timely operators to implement Bytewax's
//!     behavior.
//!
//!     2. Shim functions that totally encapsulate PyO3 and Python
//!     calling boilerplate to easily pass into Timely operators.

use crate::dataflow::StepId;
use crate::recovery::RecoveryStore;
use crate::recovery::UpdateType;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Antichain;
use timely::progress::Timestamp;
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
    /// First return stream is reduction output, second is aggregator
    /// updates for recovery you'll probably pass into a [`Backup`]
    /// operator.
    fn reduce<
        R: Fn(&K, V, V) -> V + 'static,  // Reducer
        C: Fn(&K, &V) -> bool + 'static, // Is reduction complete and send aggregator downstream?
        H: Fn(&K) -> u64 + 'static,      // Hash function of key to worker
    >(
        &self,
        state_cache: HashMap<K, V>,
        reducer: R,
        is_complete: C,
        hasher: H,
    ) -> (Stream<S, (K, V)>, Stream<S, (K, Option<V>)>)
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
        mut state_cache: HashMap<K, V>,
        reducer: R,
        is_complete: C,
        hasher: H,
    ) -> (Stream<S, (K, V)>, Stream<S, (K, Option<V>)>) {
        let mut op_builder = OperatorBuilder::new("Reduce".to_owned(), self.scope());

        let mut input = op_builder.new_input(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
        );

        let (mut downstream_output_wrapper, downstream_stream) = op_builder.new_output();
        let (mut state_update_output_wrapper, state_update_stream) = op_builder.new_output();

        op_builder.build(move |_init_capabilities| {
            let mut tmp_key_values = Vec::new();
            let mut incoming_epoch_to_key_values_buffer = HashMap::new();

            let mut tmp_updated_keys = HashSet::new();
            let mut outgoing_epoch_to_state_updates_buffer = HashMap::new();

            let mut downstream_fncater = FrontierNotificator::new();
            let mut state_update_fncater = FrontierNotificator::new();
            move |input_frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut downstream_output_handle = downstream_output_wrapper.activate();
                let mut state_update_output_handle = state_update_output_wrapper.activate();

                input_handle.for_each(|cap, key_values| {
                    key_values.swap(&mut tmp_key_values);

                    let epoch = cap.time();
                    incoming_epoch_to_key_values_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_key_values.drain(..));

                    downstream_fncater.notify_at(cap.delayed_for_output(epoch, 0));
                    state_update_fncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                downstream_fncater.for_each(
                    &[input_handle.frontier()],
                    |downstream_cap, _ncater| {
                        let epoch = downstream_cap.time();
                        if let Some(key_values) = incoming_epoch_to_key_values_buffer.remove(epoch)
                        {
                            for (key, value) in key_values {
                                let aggregator_for_key = state_cache.remove(&key);

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

                                // Save the aggregator so we can use
                                // it if there are multiple values
                                // within this epoch. We do not emit
                                // updated state here because we don't
                                // have finalized state for the epoch.
                                if is_complete(&key, &updated_aggregator_for_key) {
                                    let mut downstream_output_session =
                                        downstream_output_handle.session(&downstream_cap);
                                    downstream_output_session
                                        .give((key.clone(), updated_aggregator_for_key));
                                    state_cache.remove(&key);
                                } else {
                                    state_cache.insert(key.clone(), updated_aggregator_for_key);
                                }
                                // Note which keys have had their state
                                // modified. Write that updated state to
                                // the recovery store once the epoch is
                                // complete.
                                tmp_updated_keys.insert(key);
                            }

                            // Now that this epoch is over, find the final
                            // updated state for all keys that were
                            // touched and save that to be emitted
                            // downstream for the second output.
                            for key in tmp_updated_keys.drain() {
                                let updated_state = state_cache.get(&key).cloned();
                                outgoing_epoch_to_state_updates_buffer
                                    .entry(epoch.clone())
                                    .or_insert_with(Vec::new)
                                    .push((key, updated_state));
                            }
                        }
                    },
                );

                state_update_fncater.for_each(
                    &[input_handle.frontier()],
                    |state_update_cap, _ncater| {
                        let epoch = state_update_cap.time();
                        let mut state_update_output_session =
                            state_update_output_handle.session(&state_update_cap);
                        // Now that this epoch is complete, write out any
                        // modified state. `remove()` will ensure we
                        // slowly drain the buffer.
                        if let Some(state_updates) =
                            outgoing_epoch_to_state_updates_buffer.remove(epoch)
                        {
                            for (key, updated_state) in state_updates {
                                state_update_output_session.give((key, updated_state));
                            }
                        }
                    },
                );
            }
        });

        (downstream_stream, state_update_stream)
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
    /// First return stream is output, second is state updates for
    /// recovery you'll probably pass into a [`Backup`] operator.
    fn stateful_map<
        R: Data,                                     // Output item type
        D: Data,                                     // Per-key state
        I: IntoIterator<Item = R>,                   // Iterator of output items
        B: Fn(&K) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&K, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        state_cache: HashMap<K, D>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> (Stream<S, (K, R)>, Stream<S, (K, Option<D>)>)
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> StatefulMap<S, K, V>
    for Stream<S, (K, V)>
{
    fn stateful_map<
        R: Data,                                     // Output item type
        D: Data,                                     // Per-key state
        I: IntoIterator<Item = R>,                   // Iterator of output items
        B: Fn(&K) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&K, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&K) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        mut state_cache: HashMap<K, D>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> (Stream<S, (K, R)>, Stream<S, (K, Option<D>)>) {
        let mut op_builder = OperatorBuilder::new("StatefulMap".to_owned(), self.scope());

        let mut input = op_builder.new_input(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| hasher(key)),
        );

        let (mut downstream_output_wrapper, downstream_stream) = op_builder.new_output();
        let (mut state_update_output_wrapper, state_update_stream) = op_builder.new_output();

        op_builder.build(move |_init_capabilities| {
            let mut tmp_key_values = Vec::new();
            let mut incoming_epoch_to_key_values_buffer = HashMap::new();

            let mut tmp_updated_keys = HashSet::new();
            let mut outgoing_epoch_to_state_updates_buffer = HashMap::new();

            // We have to jump through hoops to retain separate
            // capabilities per-output. See
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            let mut downstream_fncater = FrontierNotificator::new();
            let mut state_update_fncater = FrontierNotificator::new();
            move |input_frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut downstream_output_handle = downstream_output_wrapper.activate();
                let mut state_update_output_handle = state_update_output_wrapper.activate();

                input_handle.for_each(|cap, key_values| {
                    key_values.swap(&mut tmp_key_values);

                    let epoch = cap.time();
                    incoming_epoch_to_key_values_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_key_values.drain(..));

                    // AFAICT, ports are in order of `new_output()`
                    // calls. Also I don't understand how to use
                    // `retain_for_output()` multiple times since it
                    // takes an owned
                    // capability. `delayed_for_output()` seems to
                    // work but might be incorrect?
                    downstream_fncater.notify_at(cap.delayed_for_output(epoch, 0));
                    state_update_fncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                // This runs once per epoch that will be no longer be
                // seen in the input of this operator (not the whole
                // dataflow, though).
                downstream_fncater.for_each(
                    &[input_handle.frontier()],
                    |downstream_cap, _ncater| {
                        let epoch = downstream_cap.time();
                        if let Some(key_values) = incoming_epoch_to_key_values_buffer.remove(epoch)
                        {
                            let mut downstream_output_session =
                                downstream_output_handle.session(&downstream_cap);
                            for (key, value) in key_values {
                                let state_for_key = state_cache
                                    .remove(&key)
                                    // If there's no previous state, build
                                    // anew.
                                    .unwrap_or_else(|| builder(&key));

                                let (updated_state_for_key, output) =
                                    mapper(&key, state_for_key, value);

                                downstream_output_session.give_iterator(
                                    output.into_iter().map(|item| (key.clone(), item)),
                                );

                                // Save the state so we can use it if
                                // there are multiple values within
                                // this epoch. We do not emit updated
                                // state here because we don't have
                                // finalized state for the epoch.
                                if let Some(updated_state) = updated_state_for_key {
                                    state_cache.insert(key.clone(), updated_state);
                                } else {
                                    state_cache.remove(&key);
                                }
                                // Note which keys have had their state
                                // updated.
                                tmp_updated_keys.insert(key);
                            }

                            // Now that this epoch is over, find the final
                            // updated state for all keys that were
                            // touched and save that to be emitted
                            // downstream for the second output.
                            for key in tmp_updated_keys.drain() {
                                let updated_state = state_cache.get(&key).cloned();
                                outgoing_epoch_to_state_updates_buffer
                                    .entry(epoch.clone())
                                    .or_insert_with(Vec::new)
                                    .push((key, updated_state));
                            }
                        }
                    },
                );

                state_update_fncater.for_each(
                    &[input_handle.frontier()],
                    |state_update_cap, _ncater| {
                        let epoch = state_update_cap.time();
                        let mut state_update_output_session =
                            state_update_output_handle.session(&state_update_cap);
                        // Now that this epoch is complete, write out any
                        // modified state. `remove()` will ensure we
                        // slowly drain the buffer.
                        if let Some(state_updates) =
                            outgoing_epoch_to_state_updates_buffer.remove(epoch)
                        {
                            for (key, updated_state) in state_updates {
                                state_update_output_session.give((key, updated_state));
                            }
                        }
                    },
                );
            }
        });

        (downstream_stream, state_update_stream)
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

/// Utility operator which calculates the dataflow frontier.
///
/// Connect to the backup output of the [`Recovery`] operator and all
/// captures in the dataflow during building.
///
/// It outputs a kind of "heartbeat" `()` but the epochs attached to
/// that will be the dataflow frontiers.
pub(crate) trait DataflowFrontier<S: Scope, K: ExchangeData + Hash + Eq> {
    fn dataflow_frontier<IC>(
        &self,
        backup: Stream<S, (StepId, K, UpdateType)>,
        captures: IC,
    ) -> Stream<S, ()>
    where
        IC: IntoIterator<Item = Stream<S, TdPyAny>>;
}

impl<S: Scope, K: ExchangeData + Hash + Eq> DataflowFrontier<S, K> for S {
    fn dataflow_frontier<IC>(
        &self,
        backup: Stream<S, (StepId, K, UpdateType)>,
        captures: IC,
    ) -> Stream<S, ()>
    where
        IC: IntoIterator<Item = Stream<S, TdPyAny>>,
    {
        let mut op_builder = OperatorBuilder::new("DataflowFrontier".to_string(), self.clone());

        let mut backup_input = op_builder.new_input(&backup, pact::Pipeline);
        let mut capture_inputs = captures
            .into_iter()
            .map(|s| op_builder.new_input(&s, pact::Pipeline))
            .collect::<Vec<_>>();

        let (mut output_wrapper, stream) = op_builder.new_output();

        op_builder.build(move |_init_capabilities| {
            let mut fncater = FrontierNotificator::new();
            move |input_frontiers| {
                let mut output_handle = output_wrapper.activate();

                backup_input.for_each(|cap, _data| {
                    fncater.notify_at(cap.retain());
                });
                for input in capture_inputs.iter_mut() {
                    input.for_each(|cap, _data| {
                        fncater.notify_at(cap.retain());
                    });
                }

                // I don't get
                // this. https://stackoverflow.com/questions/37797242/how-to-get-a-slice-of-references-from-a-vector-in-rust
                let tmp_input_frontiers = &input_frontiers.iter().collect::<Vec<_>>();
                fncater.for_each(tmp_input_frontiers, |cap, _ncater| {
                    output_handle.session(&cap).give(());
                });
            }
        });

        stream
    }
}

/// In-memory summary of all keys this worker's recovery store knows
/// about.
///
/// This is used to quickly find garbage state without needing to
/// query the recovery store itself.
struct RecoveryStoreSummary<K, T> {
    db: HashMap<(StepId, K), HashMap<T, UpdateType>>,
}

impl<K: ExchangeData + Hash + Eq, T: Timestamp> RecoveryStoreSummary<K, T> {
    fn new() -> Self {
        Self { db: HashMap::new() }
    }

    /// Mark that state for this step ID, key, and epoch was saved.
    ///
    /// We don't store the state itself in-memory, just the update
    /// type (for GC reasons).
    fn insert_saved(&mut self, step_id: StepId, key: K, epoch: T, state: UpdateType) {
        self.db
            .entry((step_id, key))
            .or_default()
            .insert(epoch, state);
    }

    /// Find and remove all garbage given a finalized epoch.
    ///
    /// Garbage is any state data before or during a finalized epoch,
    /// other than the last upsert for a key (since that's still
    /// relevant since it hasn't been overwritten yet).
    fn drain_garbage(&mut self, finalized_epoch: &T) -> Vec<(StepId, K, T)> {
        let mut garbage = Vec::new();

        let mut empty_map_keys = Vec::new();
        for (map_key, epoch_updates) in self.db.iter_mut() {
            let (step_id, key) = map_key;

            // TODO: The following becomes way cleaner once
            // [`std::collections::BTreeMap::drain_filter`] hits
            // stable.

            // [`std::collections::BTreeMap::split_off`] looks at all
            // the keys (epochs) in order, keeps every epoch <
            // finalized_epoch, returns epoch >= finalized_epoch.
            let (mut map_key_garbage, mut map_key_non_garbage): (Vec<_>, Vec<_>) = epoch_updates
                .drain()
                .partition(|(epoch, _update_type)| epoch <= finalized_epoch);
            map_key_garbage.sort();

            // If the final bit of "garbage" is an upsert, keep it,
            // since it's the state we'd use to recover.
            if let Some(epoch_update) = map_key_garbage.pop() {
                let (_epoch, update_type) = &epoch_update;
                if update_type == &UpdateType::Upsert {
                    map_key_non_garbage.push(epoch_update);
                } else {
                    map_key_garbage.push(epoch_update);
                }
            }

            for (epoch, _update_type) in map_key_garbage {
                garbage.push((step_id.clone(), key.clone(), epoch.clone()));
            }

            // Non-garbage should remain in the in-mem DB.
            *epoch_updates = map_key_non_garbage.into_iter().collect::<HashMap<_, _>>();

            if epoch_updates.is_empty() {
                empty_map_keys.push(map_key.clone());
            }
        }

        // Clean up any keys that aren't seen again.
        for map_key in empty_map_keys {
            self.db.remove(&map_key);
        }

        garbage
    }
}

/// Utility operator which deals with all recovery store coordination.
///
/// Does three things:
///
/// 1. Listens to the streams of all state updates from stateful
/// operators and backs them up to the recovery store. It then outputs
/// a summary of backed up state.
///
/// 2. Listens to the dataflow frontier, determines if any previously
/// backed-up state is garbage and GCs it.
///
/// 3. Writes out the current dataflow frontier as the recovery epoch.
pub(crate) trait Recovery<S: Scope, K: ExchangeData + Hash + Eq, D: Data> {
    fn recovery<IB>(
        &self,
        state_updates: IB,
        dataflow_frontier_loop: Stream<S, ()>,
        recovery_store: Box<dyn RecoveryStore<S::Timestamp, K, D>>,
        recovery_store_summary: Vec<(StepId, K, S::Timestamp, UpdateType)>,
    ) -> (
        Stream<S, (StepId, K, UpdateType)>,
        Stream<S, (StepId, K, S::Timestamp)>,
    )
    where
        IB: IntoIterator<Item = Stream<S, (StepId, K, Option<D>)>>;
}

impl<S: Scope, K: ExchangeData + Hash + Eq + Debug, D: Data> Recovery<S, K, D> for S
// TODO: Drop the Timestamp = u64 requirement. The only thing
// preventing that is some way of specifying a generic path summary
// that means "timestamp not incremented" like
// `Antichain::from_elem(0)`.
where
    S: Scope<Timestamp = u64>,
{
    fn recovery<IB>(
        &self,
        state_updates: IB,
        dataflow_frontier_loop: Stream<S, ()>,
        mut recovery_store: Box<dyn RecoveryStore<S::Timestamp, K, D>>,
        recovery_store_log: Vec<(StepId, K, S::Timestamp, UpdateType)>,
    ) -> (
        Stream<S, (StepId, K, UpdateType)>,
        Stream<S, (StepId, K, S::Timestamp)>,
    )
    where
        IB: IntoIterator<Item = Stream<S, (StepId, K, Option<D>)>>,
    {
        let mut op_builder = OperatorBuilder::new("Recovery".to_string(), self.clone());

        let (mut backup_output_wrapper, backup_output_stream) = op_builder.new_output();
        let (mut gc_output_wrapper, gc_output_stream) = op_builder.new_output();

        // We have to use these complicated sigatures to ensure the
        // path summaries are correct because not every input results
        // in output on every output.
        let mut dataflow_frontier_input = op_builder.new_input_connection(
            &dataflow_frontier_loop,
            pact::Pipeline,
            // This is saying this input only produces output on the
            // second / GC output.
            vec![Antichain::new(), Antichain::from_elem(0)],
        );
        let mut state_inputs = state_updates
            .into_iter()
            .map(|s| {
                op_builder.new_input_connection(
                    &s,
                    pact::Pipeline,
                    // This is saying this input only produces output
                    // on the first / backup output.
                    vec![Antichain::from_elem(0), Antichain::new()],
                )
            })
            .collect::<Vec<_>>();

        let mut gc_fncater = FrontierNotificator::new();
        op_builder.build(move |_init_capabilities| {
            let mut tmp_state_updates = Vec::new();
            let mut tmp_dataflow_frontier_pings = Vec::new();
            let mut recovery_store_summary = RecoveryStoreSummary::new();

            // Load the initial summary of the recovery store into
            // this special query structure in memory.
            for (step_id, key, epoch, update_type) in recovery_store_log {
                recovery_store_summary.insert_saved(step_id, key, epoch, update_type);
            }

            move |input_frontiers| {
                // Backup section: continuously backup state updates
                // to the recovery store and then emit downstream a
                // summary of what happened.
                let mut backup_output_handler = backup_output_wrapper.activate();
                for input in state_inputs.iter_mut() {
                    input.for_each(|cap, data| {
                        data.swap(&mut tmp_state_updates);
                        let cap = cap.retain_for_output(0);
                        let mut backup_output_session = backup_output_handler.session(&cap);
                        let epoch = cap.time();

                        // Drain items so we don't have to re-allocate.
                        for (step_id, key, state) in tmp_state_updates.drain(..) {
                            recovery_store.save_state(&step_id, &key, epoch, &state);
                            let update_type: UpdateType = state.into();
                            recovery_store_summary.insert_saved(
                                step_id.clone(),
                                key.clone(),
                                *epoch,
                                update_type.clone(),
                            );
                            backup_output_session.give((step_id, key, update_type));
                        }
                    });
                }

                // Store in the notificator what epochs have resulted
                // in backup data.
                dataflow_frontier_input.for_each(|cap, data| {
                    // Drain the input so the frontier advances.
                    data.swap(&mut tmp_dataflow_frontier_pings);
                    tmp_dataflow_frontier_pings.drain(..);
                    gc_fncater.notify_at(cap.retain_for_output(1));
                });

                // Find epochs that are now older than the dataflow
                // frontier. These are now finalized and might be able
                // to result in garbage.
                let mut gc_output_handler = gc_output_wrapper.activate();
                // 0th is the first defined input.
                let dataflow_frontier = &input_frontiers[0];
                gc_fncater.for_each(&[dataflow_frontier], |cap, _ncater| {
                    let mut gc_output_session = gc_output_handler.session(&cap);
                    // If the dataflow frontier has passed a
                    // notificator-retained epoch, it means it is
                    // fully output and backed up.
                    let finalized_epoch = cap.time();

                    // Resume from the beginning of the dataflow
                    // frontier; it's the oldest in-progress epoch.
                    // We save the resume epoch here in the garbage
                    // collector and not in the dataflow frontier
                    // operator because we are downstream of
                    // broadcast() which ensures we'll be getting the
                    // true cluster dataflow frontier, not local to
                    // any worker.
                    recovery_store.save_frontier(dataflow_frontier.frontier());

                    // Now remove all GCd items from the recovery
                    // store and the local copy of the keyspace.
                    for (step_id, key, epoch) in
                        recovery_store_summary.drain_garbage(finalized_epoch)
                    {
                        recovery_store.delete_state(&step_id, &key, &epoch);
                        // Output that we deleted this previous state
                        // in the finalized epoch.
                        gc_output_session.give((step_id, key, epoch));
                    }
                });

                // NOTE: We won't call this GC code when the dataflow
                // frontier closes / input is complete. This makes
                // sense to me: It's not correct to say last_epoch+1
                // has been "finalized" as it never happened. And it
                // supports the use case of "continuing" a completed
                // dataflow by starting back up at that epoch.
            }
        });

        (backup_output_stream, gc_output_stream)
    }
}
