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
use crate::log_func;
use crate::pyo3_extensions::StateKey;
use crate::pyo3_extensions::{TdPyAny, TdPyCallable, TdPyIterator};
use crate::recovery::ProgressReader;
use crate::recovery::ProgressWriter;
use crate::recovery::RecoveryKey;
use crate::recovery::StateBackup;
use crate::recovery::StateReader;
use crate::recovery::StateUpdate;
use crate::recovery::StateWriter;
use crate::recovery::{FrontierUpdate, StateCollector};
use crate::with_traceback;
use log::debug;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::thread;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::flow_controlled::iterator_source;
use timely::dataflow::operators::flow_controlled::IteratorSourceInput;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::FrontierNotificator;
use timely::dataflow::operators::Operator;
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

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

pub(crate) fn build_none() -> TdPyAny {
    Python::with_gil(|py| py.None()).into()
}

pub(crate) fn reduce(
    reducer: &TdPyCallable,
    is_complete: &TdPyCallable,
    key: &StateKey,
    acc: TdPyAny,
    value: TdPyAny,
) -> (Option<TdPyAny>, impl IntoIterator<Item = TdPyAny>) {
    Python::with_gil(|py| {
        let updated_acc: TdPyAny = if acc.is_none(py) {
            value
        } else {
            let updated_acc = with_traceback!(
                py,
                reducer.call1(py, (acc.clone_ref(py), value.clone_ref(py)))
            )
            .into();
            debug!("reduce for key={key:?}: reducer={reducer:?}(acc={acc:?}, value={value:?}) -> updated_acc={updated_acc:?}",);
            updated_acc
        };

        let should_emit_and_discard_acc: bool = with_traceback!(
            py,
            is_complete
                .call1(py, (updated_acc.clone_ref(py),))?
                .extract(py)
        );
        debug!("reduce for key={key:?}: is_complete={is_complete:?}(updated_acc={updated_acc:?}) -> should_emit_and_discard_acc={should_emit_and_discard_acc:?}");

        if should_emit_and_discard_acc {
            (None, Some(updated_acc))
        } else {
            (Some(updated_acc), None)
        }
    })
}

pub(crate) fn reduce_epoch(
    reducer: &TdPyCallable,
    acc: &mut Option<TdPyAny>,
    _key: &StateKey,
    value: TdPyAny,
) {
    debug!(
        "{}, reducer:{:?}, key:{:?}, value:{:?}, acc:{:?}",
        log_func!(),
        reducer,
        _key,
        value,
        acc
    );
    Python::with_gil(|py| {
        let updated_acc = match acc {
            Some(acc) => with_traceback!(py, reducer.call1(py, (acc.clone_ref(py), value))).into(),
            None => value,
        };
        *acc = Some(updated_acc);
        debug!(
            "{}, reducer:{:?}, key:{:?}, updated_acc:{:?}",
            log_func!(),
            reducer,
            _key,
            acc
        );
    });
}

pub(crate) fn reduce_epoch_local(
    reducer: &TdPyCallable,
    accs: &mut HashMap<StateKey, TdPyAny>,
    all_key_value_in_epoch: &Vec<(StateKey, TdPyAny)>,
) {
    Python::with_gil(|py| {
        let _current = thread::current();
        let id = _current.id();
        for (key, value) in all_key_value_in_epoch {
            let acc = accs.entry(key.clone());
            debug!(
                "thread:{:?}, {}, reducer:{:?}, key:{:?}, value:{:?}, acc:{:?}",
                id,
                log_func!(),
                reducer,
                key,
                value,
                acc
            );
            acc.and_modify(|acc| {
                *acc = with_traceback!(py, reducer.call1(py, (acc.clone_ref(py), value))).into();
                debug!(
                    "{}, reducer:{:?}, key:{:?}, value:{:?}, updated_acc:{:?}",
                    log_func!(),
                    reducer,
                    key,
                    value,
                    *acc
                );
            })
            .or_insert(value.clone_ref(py));
        }
    });
}

/// Extension trait for [`Stream`].
// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulMap<S: Scope, V: ExchangeData> {
    /// See [`crate::dataflow::Dataflow::stateful_map`] for semantics.
    ///
    /// All stateful operators that need recovery are implemented in
    /// terms of this so that we only need to write the recovery
    /// system interop once.
    ///
    /// For recovery, this takes a state loading stream, where you can
    /// pipe in all state updates from a previous execution to
    /// resume. It also emits downstream [`StateBackup`] events to be
    /// backed up.
    fn stateful_map<
        R: Data,                                            // Output item type
        D: ExchangeData,                                    // Per-key state
        I: IntoIterator<Item = R>,                          // Iterator of output items
        B: Fn(&StateKey) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&StateKey, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&StateKey) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        state_loading_stream: Stream<S, StateBackup<S::Timestamp, D>>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> (
        Stream<S, (StateKey, R)>,
        Stream<S, StateBackup<S::Timestamp, D>>,
    )
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, V: ExchangeData> StatefulMap<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_map<
        R: Data,                                            // Output item type
        D: ExchangeData,                                    // Per-key state
        I: IntoIterator<Item = R>,                          // Iterator of output items
        B: Fn(&StateKey) -> D + 'static,                    // Builder of new per-key state
        M: Fn(&StateKey, D, V) -> (Option<D>, I) + 'static, // Mapper
        H: Fn(&StateKey) -> u64 + 'static,                  // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        state_loading_stream: Stream<S, StateBackup<S::Timestamp, D>>,
        builder: B,
        mapper: M,
        hasher: H,
    ) -> (
        Stream<S, (StateKey, R)>,
        Stream<S, StateBackup<S::Timestamp, D>>,
    ) {
        let mut op_builder = OperatorBuilder::new("StatefulMap".to_owned(), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut state_backup_wrapper, state_backup_stream) = op_builder.new_output();

        let hasher = Rc::new(hasher);
        let input_hasher = hasher.clone();
        let mut input = op_builder.new_input_connection(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| input_hasher(key)),
            // This is saying this input results in items on either
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
        );
        let loading_hasher = hasher.clone();
        let mut state_loading_input = op_builder.new_input_connection(
            &state_loading_stream,
            pact::Exchange::new(
                move |StateBackup(RecoveryKey(ref _step_id, ref key, ref _epoch), ref _state)| {
                    loading_hasher(key)
                },
            ),
            // This is saying this input results in items on neither
            // output.
            vec![Antichain::new(), Antichain::new()],
        );

        op_builder.build(move |_init_capabilities| {
            // This stores the working map of key to value on the
            // frontier. We update this in-place when the frontier
            // progresses and then emit the results.
            let mut state_cache = HashMap::new();

            let mut tmp_backups = Vec::new();
            let mut incoming_epoch_to_backups_buffer = HashMap::new();

            let mut tmp_key_values = Vec::new();
            let mut incoming_epoch_to_key_values_buffer = HashMap::new();

            let mut outgoing_epoch_to_key_updates_buffer = HashMap::new();

            // We have to jump through hoops to retain separate
            // capabilities per-output. See
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            let mut state_loading_fncater = FrontierNotificator::new();
            let mut output_fncater = FrontierNotificator::new();
            let mut state_backup_fncater = FrontierNotificator::new();

            move |input_frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut state_loading_handle =
                    FrontieredInputHandle::new(&mut state_loading_input, &input_frontiers[1]);

                let mut output_handle = output_wrapper.activate();
                let mut state_backup_handle = state_backup_wrapper.activate();

                // Store the state backups from the loading input in a
                // buffer under the epoch until that frontier has
                // passed.
                state_loading_handle.for_each(|cap, backups| {
                    let epoch = cap.time();
                    backups.swap(&mut tmp_backups);

                    incoming_epoch_to_backups_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_backups.drain(..));

                    state_loading_fncater.notify_at(cap.retain_for_output(1));
                });

                // Store the input items in a buffer under the epoch
                // until that frontier has passed.
                input_handle.for_each(|cap, key_values| {
                    let epoch = cap.time();
                    key_values.swap(&mut tmp_key_values);

                    incoming_epoch_to_key_values_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_key_values.drain(..));

                    // Input items will evolve the state and both emit
                    // something downstream and result in a state
                    // update.

                    // AFAICT, ports are in order of `new_output()`
                    // calls. Also I don't understand how to use
                    // `retain_for_output()` multiple times since it
                    // takes an owned
                    // capability. `delayed_for_output()` seems to
                    // work but might be incorrect?
                    output_fncater.notify_at(cap.delayed_for_output(epoch, 0));
                    state_backup_fncater.notify_at(cap.delayed_for_output(epoch, 1));
                });

                // Apply state loads first.
                state_loading_fncater.for_each(
                    &[input_handle.frontier(), state_loading_handle.frontier()],
                    |state_update_cap, _ncater| {
                        let epoch = state_update_cap.time();

                        if let Some(backups) = incoming_epoch_to_backups_buffer.remove(epoch) {
                            for StateBackup(RecoveryKey(_step_id, key, _epoch), state_update) in
                                backups
                            {
                                // Input on the state input fully
                                // overwrites state for that key.
                                match state_update {
                                    StateUpdate::Upsert(state) => state_cache.insert(key, state),
                                    StateUpdate::Reset => state_cache.remove(&key),
                                };

                                // Don't output state loads downstream
                                // as backups.
                            }
                        }
                    },
                );

                // Then run stateful map functionality.
                output_fncater.for_each(
                    &[input_handle.frontier(), state_loading_handle.frontier()],
                    |downstream_cap, _ncater| {
                        let epoch = downstream_cap.time();

                        if let Some(key_values) = incoming_epoch_to_key_values_buffer.remove(epoch)
                        {
                            let mut output_session = output_handle.session(&downstream_cap);

                            for (key, value) in key_values {
                                let state_for_key = state_cache
                                    .remove(&key)
                                    // If there's no previous state, build
                                    // anew.
                                    .unwrap_or_else(|| builder(&key));

                                let (updated_state_for_key, output) =
                                    mapper(&key, state_for_key, value);
                                let state_update = match updated_state_for_key {
                                    Some(state) => StateUpdate::Upsert(state),
                                    None => StateUpdate::Reset,
                                };

                                output_session.give_iterator(
                                    output.into_iter().map(|item| (key.clone(), item)),
                                );

                                // Save the state so we can use it if
                                // there are multiple values within
                                // this epoch. We do not emit updated
                                // state here because we don't have
                                // finalized state for the epoch.
                                match &state_update {
                                    StateUpdate::Upsert(state) => {
                                        state_cache.insert(key.clone(), state.clone())
                                    }
                                    StateUpdate::Reset => state_cache.remove(&key),
                                };

                                outgoing_epoch_to_key_updates_buffer
                                    .entry(epoch.clone())
                                    .or_insert_with(HashMap::new)
                                    .insert(key, state_update);
                            }
                        }
                    },
                );

                // Then output final state update.
                state_backup_fncater.for_each(
                    &[input_handle.frontier(), state_loading_handle.frontier()],
                    |state_update_cap, _ncater| {
                        let epoch = state_update_cap.time();

                        let mut state_backup_session =
                            state_backup_handle.session(&state_update_cap);

                        // Now that this epoch is complete, write out any
                        // modified state. `remove()` will ensure we
                        // slowly drain the buffer.
                        if let Some(key_updates) =
                            outgoing_epoch_to_key_updates_buffer.remove(epoch)
                        {
                            let backups = key_updates.into_iter().map(|(key, update)| {
                                StateBackup(
                                    RecoveryKey(step_id.clone(), key, epoch.clone()),
                                    update,
                                )
                            });
                            state_backup_session.give_iterator(backups);
                        }
                    },
                );
            }
        });

        (output_stream, state_backup_stream)
    }
}

pub(crate) fn build(builder: &TdPyCallable, key: &StateKey) -> TdPyAny {
    Python::with_gil(|py| {
        let initial_state = with_traceback!(py, builder.call1(py, (key.clone(),))).into();
        debug!("build for key={key:?}: builder={builder:?}(key={key:?}) -> initial_state{initial_state:?}");
        initial_state
    })
}

pub(crate) fn stateful_map(
    mapper: &TdPyCallable,
    key: &StateKey,
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

/// Extension trait for [`Stream`].
pub(crate) trait WriteState<S: Scope, D: Data> {
    /// Writes state backups in timestamp order.
    fn write_state_with(
        &self,
        state_writer: Box<dyn StateWriter<S::Timestamp, D>>,
    ) -> Stream<S, StateBackup<S::Timestamp, D>>;
}

impl<S: Scope, D: Data> WriteState<S, D> for Stream<S, StateBackup<S::Timestamp, D>> {
    fn write_state_with(
        &self,
        mut state_writer: Box<dyn StateWriter<S::Timestamp, D>>,
    ) -> Stream<S, StateBackup<S::Timestamp, D>> {
        self.unary_notify(pact::Pipeline, "WriteState", None, {
            // TODO: Store worker_index in the backup so we know if we
            // crossed the worker backup streams?

            // let worker_index = self.scope().index();

            let mut tmp_backups = Vec::new();
            let mut epoch_to_backups_buffer = HashMap::new();

            move |input, output, ncater| {
                input.for_each(|cap, backups| {
                    let epoch = cap.time();
                    backups.swap(&mut tmp_backups);

                    epoch_to_backups_buffer
                        .entry(epoch.clone())
                        .or_insert_with(Vec::new)
                        .extend(tmp_backups.drain(..));

                    ncater.notify_at(cap.retain());
                });

                ncater.for_each(|cap, _count, _ncater| {
                    let epoch = cap.time();
                    if let Some(mut backups) = epoch_to_backups_buffer.remove(epoch) {
                        for backup in &backups {
                            state_writer.write(backup);
                        }

                        output.session(&cap).give_vec(&mut backups);
                    }
                });
            }
        })
    }
}

/// Extension trait for [`Stream`].
pub(crate) trait WriteProgress<S: Scope, D: Data> {
    /// Write out the current frontier of the output this is connected
    /// to whenever it changes.
    fn write_progress_with(
        &self,
        frontier_writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> Stream<S, D>;
}

impl<S: Scope, D: Data> WriteProgress<S, D> for Stream<S, D> {
    fn write_progress_with(
        &self,
        mut frontier_writer: Box<dyn ProgressWriter<S::Timestamp>>,
    ) -> Stream<S, D> {
        self.unary_notify(pact::Pipeline, "WriteProgress", None, {
            let mut tmp_data = Vec::new();

            move |input, output, ncater| {
                input.for_each(|cap, data| {
                    data.swap(&mut tmp_data);

                    output.session(&cap).give_vec(&mut tmp_data);

                    ncater.notify_at(cap.retain());
                });

                ncater.for_each(|_cap, _count, ncater| {
                    // 0 is our singular input.
                    let frontier = ncater.frontier(0).to_owned();

                    // Don't write out the last "empty" frontier to
                    // allow restarting from the end of the dataflow.
                    if !frontier.elements().is_empty() {
                        let update = FrontierUpdate(frontier.to_owned());

                        frontier_writer.write(&update);
                    }
                });
            }
        })
    }
}

/// The [`RecoveryStoreSummary`] doesn't need to retain full copies of
/// state to determine what is garbage (just that there was a reset or
/// an update), so have a little enum here to represent that.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum UpdateType {
    Upsert,
    Reset,
}

impl<D> From<StateUpdate<D>> for UpdateType {
    fn from(update: StateUpdate<D>) -> Self {
        match update {
            StateUpdate::Upsert(..) => Self::Upsert,
            StateUpdate::Reset => Self::Reset,
        }
    }
}

/// In-memory summary of all keys this worker's recovery store knows
/// about.
///
/// This is used to quickly find garbage state without needing to
/// query the recovery store itself.
struct RecoveryStoreSummary<T, D> {
    db: HashMap<(StepId, StateKey), HashMap<T, UpdateType>>,
    state_type: PhantomData<D>,
}

impl<T: Timestamp, D> RecoveryStoreSummary<T, D> {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            state_type: PhantomData,
        }
    }

    /// Mark that state for this step ID, key, and epoch was backed
    /// up.
    fn insert(&mut self, backup: StateBackup<T, D>) {
        let StateBackup(RecoveryKey(step_id, key, epoch), update) = backup;
        let update_type = update.into();
        self.db
            .entry((step_id, key))
            .or_default()
            .insert(epoch, update_type);
    }

    /// Find and remove all garbage given a finalized epoch.
    ///
    /// Garbage is any state data before or during a finalized epoch,
    /// other than the last upsert for a key (since that's still
    /// relevant since it hasn't been overwritten yet).
    fn remove_garbage(&mut self, finalized_epoch: &T) -> Vec<RecoveryKey<T>> {
        let mut garbage = Vec::new();

        let mut empty_map_keys = Vec::new();
        for (map_key, epoch_updates) in self.db.iter_mut() {
            let (step_id, key) = map_key;

            // TODO: The following becomes way cleaner once
            // [`std::collections::BTreeMap::drain_filter`] and
            // [`std::collections::BTreeMap::first_entry`] hits
            // stable.

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
                garbage.push(RecoveryKey(step_id.clone(), key.clone(), epoch));
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

/// Extension trait for [`Stream`].
pub(crate) trait CollectGarbage<S: Scope, D: Data, DF: Data> {
    /// Run the recovery garbage collector.
    ///
    /// This will be instantiated on stream of already written state
    /// and will observe the dataflow's frontier and then decide what
    /// is garbage and delete it.
    ///
    /// It needs a separate handle to write to the state store so that
    /// there's not contention between it and [`WriteState`].
    fn collect_garbage(
        &self,
        state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: Stream<S, DF>,
    ) -> Stream<S, ()>;
}

impl<S: Scope, D: Data, DF: Data> CollectGarbage<S, D, DF>
    for Stream<S, StateBackup<S::Timestamp, D>>
{
    fn collect_garbage(
        &self,
        mut state_collector: Box<dyn StateCollector<S::Timestamp>>,
        dataflow_frontier: Stream<S, DF>,
    ) -> Stream<S, ()> {
        let mut op_builder = OperatorBuilder::new("CollectGarbage".to_string(), self.scope());

        let mut input = op_builder.new_input(&self, pact::Pipeline);
        let mut dataflow_frontier_input = op_builder.new_input(&dataflow_frontier, pact::Pipeline);

        let (mut output_wrapper, stream) = op_builder.new_output();

        let mut fncater = FrontierNotificator::new();
        op_builder.build(move |_init_capabilities| {
            let mut tmp_state_backups = Vec::new();
            let mut tmp_dataflow_frontier_data = Vec::new();
            let mut recovery_store_summary = RecoveryStoreSummary::new();

            move |input_frontiers| {
                let mut input = FrontieredInputHandle::new(&mut input, &input_frontiers[0]);
                let mut dataflow_frontier_input =
                    FrontieredInputHandle::new(&mut dataflow_frontier_input, &input_frontiers[1]);

                let mut output_handle = output_wrapper.activate();

                // Update our internal cache of the state store's
                // keys.
                input.for_each(|_cap, state_backups| {
                    state_backups.swap(&mut tmp_state_backups);

                    // Drain items so we don't have to re-allocate.
                    for state_backup in tmp_state_backups.drain(..) {
                        recovery_store_summary.insert(state_backup);
                    }
                });

                // Tell the notificator to trigger on dataflow
                // frontier advance.
                dataflow_frontier_input.for_each(|cap, data| {
                    // Drain the dataflow frontier input so the
                    // frontier advances.
                    data.swap(&mut tmp_dataflow_frontier_data);
                    tmp_dataflow_frontier_data.drain(..);

                    fncater.notify_at(cap.retain());
                });

                // Collect garbage.
                fncater.for_each(
                    &[input.frontier(), dataflow_frontier_input.frontier()],
                    |cap, _ncater| {
                        // If the dataflow frontier has passed a
                        // notificator-retained epoch, it means it is
                        // fully output and backed up.
                        let finalized_epoch = cap.time();

                        // Now remove all dead items from the state
                        // store and the local cache.
                        for recovery_key in recovery_store_summary.remove_garbage(finalized_epoch) {
                            state_collector.delete(&recovery_key);
                        }

                        // Note progress on the output stream.
                        output_handle.session(&cap);
                    },
                );

                // NOTE: We won't call this GC code when the dataflow
                // frontier closes / input is complete. This makes
                // sense to me: It's not correct to say last_epoch+1
                // has been "finalized" as it never happened. And it
                // supports the use case of "continuing" a completed
                // dataflow by starting back up at that epoch.
            }
        });

        stream
    }
}

/// Build a source which loads previously backed up progress data as
/// separate items under the default timestamp for this new dataflow.
///
/// Note that this pretty meta! This new _loading_ dataflow will only
/// have the zeroth epoch, but you can observe what progress was made
/// on the _previous_ dataflow.
pub(crate) fn progress_source<S: Scope, T: Data>(
    scope: &S,
    mut reader: Box<dyn ProgressReader<T>>,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, FrontierUpdate<T>>
where
    S::Timestamp: TotalOrder + Default,
{
    iterator_source(
        scope,
        "ProgressSource",
        move |last_cap| match reader.read() {
            Some(frontier_update) => {
                Some(IteratorSourceInput {
                    lower_bound: Default::default(),
                    // An iterator of (timestamp, iterator of
                    // items). Nested into iterators.
                    data: Some((S::Timestamp::default(), Some(frontier_update))),
                    target: last_cap.clone(),
                })
            }
            None => None,
        },
        probe.clone(),
    )
}

/// Build a source which loads previously backed up state data.
///
/// State must be stored in epoch order. [`WriteState`] above, does
/// that.
pub(crate) fn state_source<S: Scope, D: Data>(
    scope: &S,
    mut reader: Box<dyn StateReader<S::Timestamp, D>>,
    stop_at: S::Timestamp,
    probe: &ProbeHandle<S::Timestamp>,
) -> Stream<S, StateBackup<S::Timestamp, D>>
where
    S::Timestamp: TotalOrder + Default,
{
    iterator_source(
        scope,
        "StateSource",
        move |last_cap| match reader.read() {
            Some(state_backup) => {
                let StateBackup(RecoveryKey(_step_id, _key, epoch), _state_update) = &state_backup;
                let epoch = epoch.clone();

                if epoch < stop_at {
                    Some(IteratorSourceInput {
                        // `lower_bound` is min'd with the data's
                        // epoch so if this stays low (as is the
                        // default), you still advance.
                        lower_bound: Default::default(),
                        data: Some((epoch, Some(state_backup))),
                        target: last_cap.clone(),
                    })
                } else {
                    None
                }
            }
            None => None,
        },
        probe.clone(),
    )
}
