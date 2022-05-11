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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::Rc;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::FrontierNotificator;
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

                                // Save the aggregator so we can use it if
                                // multiple values within this epoch. We
                                // do not save to the recovery store here
                                // because we don't have finalized state
                                // for the epoch.
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
                                // multiple values within this epoch. We
                                // do not save to the recovery store here
                                // because we don't have finalized state
                                // for the epoch.
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

/// Utility operator which persists state changes coming out of the
/// stateful operators to the recovery store.
///
/// Emits a stream of update summaries for use in garbage collection.
pub(crate) trait Backup<S: Scope, K: ExchangeData + Hash + Eq, D: ExchangeData> {
    fn backup(
        &self,
        step_id: StepId,
        recovery_writer: Rc<Box<dyn RecoveryStore<S::Timestamp, K, D>>>,
    ) -> Stream<S, (StepId, K, UpdateType)>
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, D: ExchangeData> Backup<S, K, D>
    for Stream<S, (K, Option<D>)>
{
    fn backup(
        &self,
        step_id: StepId,
        recovery_writer: Rc<Box<dyn RecoveryStore<S::Timestamp, K, D>>>,
    ) -> Stream<S, (StepId, K, UpdateType)> {
        self.unary(pact::Pipeline, "Backup", |_init_capability, _info| {
            let mut items_buffer = Vec::new();
            move |input, output| {
                input.for_each(|cap, data| {
                    data.swap(&mut items_buffer);
                    let epoch = cap.time();

                    let mut output_session = output.session(&cap);
                    for (key, updated_state) in items_buffer.drain(..) {
                        recovery_writer.save_state(&step_id, &key, epoch, &updated_state);
                        output_session.give((step_id.clone(), key, updated_state.into()));
                    }
                });
            }
        })
    }
}

/// Utility operator which calculates the dataflow frontier.
///
/// Connect to all backups and captures in the dataflow during
/// building.
///
/// It outputs a kind of "heartbeat" `()` but the epochs attached to
/// that will be the dataflow frontiers.
pub(crate) trait DataflowFrontier<S: Scope, K: ExchangeData + Hash + Eq> {
    fn dataflow_frontier<IB, IC>(&self, backups: IB, captures: IC) -> Stream<S, ()>
    where
        IB: IntoIterator<Item = Stream<S, (StepId, K, UpdateType)>>,
        IC: IntoIterator<Item = Stream<S, TdPyAny>>;
}

impl<S: Scope, K: ExchangeData + Hash + Eq> DataflowFrontier<S, K> for S {
    fn dataflow_frontier<IB, IC>(&self, backups: IB, captures: IC) -> Stream<S, ()>
    where
        IB: IntoIterator<Item = Stream<S, (StepId, K, UpdateType)>>,
        IC: IntoIterator<Item = Stream<S, TdPyAny>>,
    {
        let mut op_builder = OperatorBuilder::new("DataflowFrontier".to_string(), self.clone());

        let mut backup_inputs = backups
            .into_iter()
            .map(|s| op_builder.new_input(&s, pact::Pipeline))
            .collect::<Vec<_>>();
        let mut capture_inputs = captures
            .into_iter()
            .map(|s| op_builder.new_input(&s, pact::Pipeline))
            .collect::<Vec<_>>();

        let (mut output_wrapper, stream) = op_builder.new_output();

        op_builder.build(move |_init_capabilities| {
            let mut fncater = FrontierNotificator::new();
            move |input_frontiers| {
                let mut output_handle = output_wrapper.activate();

                for input in backup_inputs.iter_mut() {
                    input.for_each(|cap, _data| {
                        fncater.notify_at(cap.retain());
                    });
                }
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

/// Utility operator which listens to the dataflow frontier and
/// streams of backup summaries and will coordinate with the recovery
/// store to garbage collect old state.
///
/// It also records the dataflow frontier.
///
/// This should be called on the dataflow frontier stream itself.
pub(crate) trait GarbageCollector<S: Scope, K: ExchangeData + Hash + Eq> {
    fn garbage_collector<IB, D: 'static>(
        &self,
        recovery_store_summary: Vec<(StepId, K, S::Timestamp, UpdateType)>,
        backups: IB,
        recovery_collector: Rc<Box<dyn RecoveryStore<S::Timestamp, K, D>>>,
    ) where
        IB: IntoIterator<Item = Stream<S, (StepId, K, UpdateType)>>;
}

impl<S, K: ExchangeData + Hash + Eq + Debug> GarbageCollector<S, K> for Stream<S, ()>
where
    S: Scope<Timestamp = u64>,
{
    fn garbage_collector<IB, D: 'static>(
        &self,
        recovery_store_log: Vec<(StepId, K, S::Timestamp, UpdateType)>,
        backups: IB,
        recovery_collector: Rc<Box<dyn RecoveryStore<S::Timestamp, K, D>>>,
    ) where
        IB: IntoIterator<Item = Stream<S, (StepId, K, UpdateType)>>,
    {
        let mut op_builder = OperatorBuilder::new("GarbageCollector".to_string(), self.scope());

        let mut dataflow_frontier_input = op_builder.new_input(self, pact::Pipeline);
        let mut backup_inputs = backups
            .into_iter()
            .map(|s| op_builder.new_input(&s, pact::Pipeline))
            .collect::<Vec<_>>();

        op_builder.build(move |_init_capabilities| {
            let mut tmp_recovery_keys = Vec::new();
            let mut garbage_key_buffer = Vec::new();
            let mut recovery_store_summary: HashMap<
                (StepId, K),
                BTreeSet<(S::Timestamp, UpdateType)>,
            > = HashMap::new();

            // Load the initial summary of the recovery store into
            // this special query structure in memory.
            for (step_id, key, epoch, update_type) in recovery_store_log {
                recovery_store_summary
                    .entry((step_id, key))
                    .or_default()
                    .insert((epoch, update_type));
            }

            move |input_frontiers| {
                let mut dataflow_frontier_input_handle =
                    FrontieredInputHandle::new(&mut dataflow_frontier_input, &input_frontiers[0]);

                // Gotta drain the data on this input to advance its
                // frontier, even if we do nothing with it.
                dataflow_frontier_input_handle.for_each(|_cap, _data| {});

                for input in backup_inputs.iter_mut() {
                    input.for_each(|cap, data| {
                        data.swap(&mut tmp_recovery_keys);
                        let epoch = cap.time();

                        // Drain items so we don't have to re-allocate.
                        for (step_id, key, update_type) in tmp_recovery_keys.drain(..) {
                            recovery_store_summary
                                .entry((step_id, key))
                                .or_default()
                                .insert((epoch.clone(), update_type));
                        }
                    });
                }

                // TODO: This runs on every operator activation. We
                // only need to run when the dataflow frontier has
                // actually updated, though.
                let dataflow_frontier = dataflow_frontier_input_handle.frontier().frontier();
                let is_collectable =
                // Why +2? The dataflow frontier is the oldest
                // in-progress epoch. Thus -1 is the epoch who's state
                // we need to fully recreate during recovery and we
                // shouldn't GC it. Thus -2 is the first GC-able
                // epoch. We'd like to be able to write
                // `dataflow_frontier - 2 >= epoch` but since there's
                // no [`Anitchain::greater_than()`], we use
                // `!less_than()` and since you can't do math on the
                // frontier set, we move the `-2` to the other side
                // and it becomes `+2`.

                // TODO: Is there a way to do this without a fixed
                // offset? It feels kinda hacky and would be cool to
                // support arbitrary timestamp types.

                    |epoch: &u64| -> bool { !dataflow_frontier.less_than(&(epoch + 2)) };
                // We save the dataflow frontier here in the garbage
                // collector and not in the dataflow frontier operator
                // because we are downstream of broadcast() which
                // ensures we'll be getting the true cluster dataflow
                // frontier, not local to any worker.
                recovery_collector.save_frontier(dataflow_frontier);

                for ((step_id, key), epoch_updates) in recovery_store_summary.iter() {
                    // [`BTreeSet::iter()`] is in asc order.
                    let mut collectable_updates = epoch_updates
                        .range(..)
                        .filter(|(epoch, _update_type)| is_collectable(epoch))
                        .map(|(epoch, update_type)| {
                            (step_id.clone(), key.clone(), *epoch, update_type.clone())
                        })
                        .collect::<Vec<_>>();

                    // Never collect the last upsert; that's the most
                    // recent recovery data! If the final update is a
                    // delete, though then collect that so abandoned
                    // keys are GCd.
                    if let Some((_step_id, _key, _epoch, UpdateType::Upsert)) =
                        collectable_updates.last()
                    {
                        collectable_updates.pop();
                    }

                    garbage_key_buffer.append(&mut collectable_updates);
                }

                // Now remove all GCd items from the recovery store
                // and the local copy of the keyspace.
                for (step_id, key, epoch, update_type) in garbage_key_buffer.drain(..) {
                    recovery_collector.delete_state(&step_id, &key, &epoch);

                    // Do this multi-step process so we don't have
                    // empty BTreeSets in the summary.
                    let recovery_key = (step_id, key);
                    let mut epochs_remaining = recovery_store_summary
                        .remove(&recovery_key)
                        .unwrap_or_default();
                    let recovery_value = (epoch, update_type);
                    epochs_remaining.remove(&recovery_value);
                    if !epochs_remaining.is_empty() {
                        recovery_store_summary.insert(recovery_key, epochs_remaining);
                    }
                }
            }
        });
    }
}
