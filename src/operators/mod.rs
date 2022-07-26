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
use crate::recovery::DeserializeBackup;
use crate::recovery::ProgressReader;
use crate::recovery::ProgressWriter;
use crate::recovery::RecoveryKey;
use crate::recovery::SerializeBackup;
use crate::recovery::StateBackup;
use crate::recovery::StateReader;
use crate::recovery::StateUpdate;
use crate::recovery::StateWriter;
use crate::recovery::{FrontierUpdate, StateCollector};
use crate::with_traceback;
use log::debug;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};
use timely::dataflow::channels::pact;
use timely::dataflow::operators::flow_controlled::iterator_source;
use timely::dataflow::operators::flow_controlled::IteratorSourceInput;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::{Filter, FrontierNotificator};
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
    Python::with_gil(|py| {
        with_traceback!(py, {
            let should_emit_pybool: TdPyAny = predicate.call1(py, (item,))?.into();
            should_emit_pybool.extract(py).map_err(|_err| PyTypeError::new_err(format!("return value of `predicate` in filter operator must be a bool; got `{should_emit_pybool:?}` instead")))
        })
    })
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

pub(crate) fn reduce(
    reducer: &TdPyCallable,
    is_complete: &TdPyCallable,
    key: &StateKey,
    acc: Option<TdPyAny>,
    next_value: Poll<Option<TdPyAny>>,
) -> (
    Option<TdPyAny>,
    impl IntoIterator<Item = TdPyAny>,
    Option<Duration>,
) {
    if let Poll::Ready(Some(value)) = next_value {
        Python::with_gil(|py| {
            let updated_acc: TdPyAny = match acc {
                // If there's no previous state for this key, use the
                // current value.
                None => value,
                Some(acc) => {
                    let updated_acc = with_traceback!(
                        py,
                        reducer.call1(py, (acc.clone_ref(py), value.clone_ref(py)))
                    )
                    .into();
                    debug!("reduce for key={key:?}: reducer={reducer:?}(acc={acc:?}, value={value:?}) -> updated_acc={updated_acc:?}",);
                    updated_acc
                }
            };

            let should_emit_and_discard_acc: bool = with_traceback!(py, {
                let should_emit_and_discard_acc_pybool: TdPyAny =
                    is_complete.call1(py, (updated_acc.clone_ref(py),))?.into();
                should_emit_and_discard_acc_pybool.extract(py).map_err(|_err| PyTypeError::new_err(format!("return value of `is_complete` in reduce operator must be a bool; got `{should_emit_and_discard_acc_pybool:?}` instead")))
            });
            debug!("reduce for key={key:?}: is_complete={is_complete:?}(updated_acc={updated_acc:?}) -> should_emit_and_discard_acc={should_emit_and_discard_acc:?}");

            if should_emit_and_discard_acc {
                (None, Some(updated_acc), None)
            } else {
                (Some(updated_acc), None, None)
            }
        })
    } else {
        (acc, None, None)
    }
}

/// Extension trait for [`Stream`].
// Based on the good work in
// https://github.com/TimelyDataflow/timely-dataflow/blob/0d0d84885672d6369a78cd9aff7beb2048390d3b/timely/src/dataflow/operators/aggregation/state_machine.rs#L57
pub(crate) trait StatefulUnary<S: Scope, V: ExchangeData> {
    /// Create a new generic stateful operator and add the required
    /// utility operators to filter and serde backup data.
    ///
    /// See [`stateful_unary_raw`] for a description of how `logic` works.
    fn stateful_unary<
        R: Data,                   // Output item type
        D: ExchangeData,           // Per-key state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Poll<Option<V>>) -> (Option<D>, I, Option<Duration>) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        logic: L,
        hasher: H,
        state_loading_stream: &Stream<S, StateBackup<S::Timestamp, Vec<u8>>>,
        state_backup_streams: &mut Vec<Stream<S, StateBackup<S::Timestamp, Vec<u8>>>>,
    ) -> Stream<S, (StateKey, R)>;

    /// Create a new generic stateful operator.
    ///
    /// This is the core Timely operator that all Bytewax stateful
    /// operators are implemented in terms of. It is awkwardly generic
    /// because of that. We do this so we only have to implement the
    /// very tricky recovery system interop once.
    ///
    /// # Logic
    ///
    /// `logic` takes three arguments and is called on new input items
    /// or after a delay timeout has expired:
    ///
    /// - The awoken key. This is due to the input stream having an
    /// item of `(key, value)` or an awakening being scheduled on that
    /// key
    ///
    /// - The last state for that key. That might be [`None`] if there
    /// wasn't any.
    ///
    /// - A possible incoming value. This has the same protocol as
    /// [`std::async_iter::AsyncIterator::poll_next`]: this logic
    /// might be awoken with no incoming values yet
    /// ([`Poll::Pending`]), a new value has arrived ([`Poll::Ready`]
    /// with a [`Some`]), or the stream has ended and there will be no
    /// more input ([`Poll::Ready`] with a [`None`]).
    ///
    /// `logic` must return a 3-tuple of:
    ///
    /// - The updated state for the key, or [`None`] if the state
    /// should be discarded.
    ///
    /// - Values to be emitted downstream. You probably only want to
    /// emit values when the incoming value is [`None`], signaling the
    /// window has closed, but you might want something more
    /// generic.
    ///
    /// - Timeout delay to be awoken after with [`Poll::Pending`] as
    /// the value. It's possible the logic will be awoken for this key
    /// earlier if new data comes in. Timeouts are not buffered across
    /// calls to logic, so you should always specify the delay to the
    /// next time you should be woken up every time.
    ///
    /// # Output
    ///
    /// The output will be a stream of `(key, value)` output
    /// 2-tuples. Values emitted by `logic` will be automatically
    /// paired with the key in the output stream.
    ///
    /// # Recovery
    ///
    /// For recovery, this requires an input of [`StateBackups`] to
    /// load, and emits a second output stream of [`StateBackups`] to
    /// be connected to the rest of the recovery machinery.
    ///
    /// These input and outputs must just be for this stateful
    /// operator.
    fn stateful_unary_raw<
        R: Data,                   // Output item type
        D: ExchangeData,           // Per-key state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Poll<Option<V>>) -> (Option<D>, I, Option<Duration>) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        state_loading_stream: Stream<S, StateBackup<S::Timestamp, D>>,
        logic: L,
        hasher: H,
    ) -> (
        Stream<S, (StateKey, R)>,
        Stream<S, StateBackup<S::Timestamp, D>>,
    )
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, V: ExchangeData> StatefulUnary<S, V> for Stream<S, (StateKey, V)>
where
    S: Scope<Timestamp = u64>,
{
    fn stateful_unary<
        R: Data,                   // Output item type
        D: ExchangeData,           // Per-key state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Poll<Option<V>>) -> (Option<D>, I, Option<Duration>) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        logic: L,
        hasher: H,
        state_loading_stream: &Stream<S, StateBackup<S::Timestamp, Vec<u8>>>,
        state_backup_streams: &mut Vec<Stream<S, StateBackup<S::Timestamp, Vec<u8>>>>,
    ) -> Stream<S, (StateKey, R)> {
        let filter_step_id = step_id.clone();
        let state_loading_stream = state_loading_stream
            .filter(
                move |StateBackup(RecoveryKey(loading_step_id, _key, _epoch), _state_update)| {
                    &filter_step_id == loading_step_id
                },
            )
            .deserialize_backup();

        let (downstream, state_backup_stream) =
            self.stateful_unary_raw(step_id, state_loading_stream, logic, hasher);

        state_backup_streams.push(state_backup_stream.serialize_backup());
        downstream
    }

    fn stateful_unary_raw<
        R: Data,                   // Output item type
        D: ExchangeData,           // Per-key state
        I: IntoIterator<Item = R>, // Iterator of output items
        L: Fn(&StateKey, Option<D>, Poll<Option<V>>) -> (Option<D>, I, Option<Duration>) + 'static, // Logic
        H: Fn(&StateKey) -> u64 + 'static, // Hash function of key to worker
    >(
        &self,
        step_id: StepId,
        state_loading_stream: Stream<S, StateBackup<S::Timestamp, D>>,
        logic: L,
        hasher: H,
    ) -> (
        Stream<S, (StateKey, R)>,
        Stream<S, StateBackup<S::Timestamp, D>>,
    ) {
        let mut op_builder = OperatorBuilder::new("StatefulUnary".to_owned(), self.scope());

        let (mut output_wrapper, output_stream) = op_builder.new_output();
        let (mut state_backup_wrapper, state_backup_stream) = op_builder.new_output();

        let hasher = Rc::new(hasher);
        let input_hasher = hasher.clone();
        let mut input_handle = op_builder.new_input_connection(
            self,
            pact::Exchange::new(move |&(ref key, ref _value)| input_hasher(key)),
            // This is saying this input results in items on either
            // output.
            vec![Antichain::from_elem(0), Antichain::from_elem(0)],
            // TODO: Figure out the magic incantation of
            // S::Timestamp::minimum() and S::Timestamp::Summary that
            // lets you do this without the trait bound S:
            // Scope<Timestamp = u64> above.
        );
        let loading_hasher = hasher.clone();
        let mut state_loading_handle = op_builder.new_input_connection(
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

        let info = op_builder.operator_info();
        let activator = self.scope().activator_for(&info.address[..]);

        op_builder.build(move |mut init_caps| {
            // This stores the working map of key to value on the
            // frontier. We update this in-place and emit the results.
            let mut state_cache = HashMap::new();

            // We have to retain separate capabilities
            // per-output. This seems to be only documented in
            // https://github.com/TimelyDataflow/timely-dataflow/pull/187
            // In reverse order because of how [`Vec::pop`] removes
            // from back.
            let mut state_backup_cap = init_caps.pop();
            let mut output_cap = init_caps.pop();

            let mut tmp_backups = Vec::new();
            let mut incoming_epoch_to_backups_buffer = HashMap::new();

            let mut tmp_key_values = Vec::new();
            let mut incoming_epoch_to_key_values_buffer = HashMap::new();

            let mut activate_epochs_buffer: Vec<S::Timestamp> = Vec::new();
            let mut key_values_buffer = Vec::new();
            let mut keys_buffer = Vec::new();
            let mut key_to_next_activate_at_buffer: HashMap<StateKey, Instant> = HashMap::new();

            let mut outgoing_epoch_to_key_updates_buffer = HashMap::new();

            move |input_frontiers| {
                // Will there be no more data?
                let eof = input_frontiers.iter().all(|f| f.is_empty());
                let is_closed = |e: &S::Timestamp| input_frontiers.iter().all(|f| !f.less_equal(e));

                if let (Some(output_cap), Some(state_backup_cap)) =
                    (output_cap.as_mut(), state_backup_cap.as_mut())
                {
                    assert!(output_cap.time() == state_backup_cap.time());
                    let mut output_handle = output_wrapper.activate();
                    let mut state_backup_handle = state_backup_wrapper.activate();

                    // Buffer the state backups and item inputs so we
                    // can apply them to the state cache in epoch
                    // order.
                    state_loading_handle.for_each(|cap, backups| {
                        let epoch = cap.time();
                        backups.swap(&mut tmp_backups);

                        incoming_epoch_to_backups_buffer
                            .entry(epoch.clone())
                            .or_insert_with(Vec::new)
                            .extend(tmp_backups.drain(..));
                    });
                    input_handle.for_each(|cap, key_values| {
                        let epoch = cap.time();
                        key_values.swap(&mut tmp_key_values);

                        incoming_epoch_to_key_values_buffer
                            .entry(epoch.clone())
                            .or_insert_with(Vec::new)
                            .extend(tmp_key_values.drain(..));

                    });

                    // TODO: Is this the right way to get the epoch
                    // from a frontier? I haven't seen any examples
                    // with non-comparable timestamps to understand
                    // this. Is the current capability reasonable for
                    // when the frontiers are closed?
                    let frontier_epoch = input_frontiers
                        .iter()
                        .flat_map(|mf| mf.frontier().iter().min().cloned())
                        .min()
                        .unwrap_or(output_cap.time().clone());

                    // Try to process all the epochs we have data for.
                    // Filter out epochs that are ahead of the
                    // frontier. The state at the beginning of those
                    // epochs are not truly known yet, so we can't
                    // apply input in those epochs yet.
                    activate_epochs_buffer.extend(incoming_epoch_to_backups_buffer.keys().cloned().filter(is_closed));
                    activate_epochs_buffer.extend(incoming_epoch_to_key_values_buffer.keys().cloned().filter(is_closed));
                    // Even if we don't have input data from an epoch,
                    // we might have some output data from the
                    // previous activation we need to output and the
                    // frontier might have already progressed and it
                    // would be stranded.
                    activate_epochs_buffer.extend(outgoing_epoch_to_key_updates_buffer.keys().cloned().filter(is_closed));
                    // Eagarly execute the frontier.
                    activate_epochs_buffer.push(frontier_epoch);
                    activate_epochs_buffer.dedup();
                    activate_epochs_buffer.sort();

                    // Drain to re-use buffer. For each epoch in
                    // order:
                    for epoch in activate_epochs_buffer.drain(..) {
                        // Since the frontier has advanced to at least
                        // this epoch (because we're going through
                        // them in order), say that we'll not be
                        // sending output at any older epochs. This
                        // also asserts "apply changes in epoch order"
                        // to the state cache.
                        output_cap.downgrade(&epoch);
                        state_backup_cap.downgrade(&epoch);

                        match (
                            incoming_epoch_to_backups_buffer.remove(&epoch),
                            incoming_epoch_to_key_values_buffer.remove(&epoch),
                        ) {
                            (Some(_), Some(_)) => panic!("Recoverable operator got both state backups and input for epoch {epoch:?}; is the resume epoch being respected?"),
                            // Either load state backup.
                            (Some(backups), None) => {
                                // There will be multiple backups per
                                // epoch, one for each key.
                                for StateBackup(RecoveryKey(loading_step_id, key, _epoch), state_update) in
                                    backups {
                                        assert!(loading_step_id == step_id);
                                        // Input on the state input
                                        // fully overwrites state for
                                        // that key.
                                        match state_update {
                                            StateUpdate::Upsert(state) => state_cache.insert(key, state),
                                            StateUpdate::Reset => state_cache.remove(&key),
                                        };

                                        // Don't output state loads
                                        // downstream as backups.
                                    }
                            }
                            // Or run logic and buffer state updates.
                            (None, incoming_key_values) => {
                                let mut output_session = output_handle.session(&output_cap);

                                // Include all the incoming data.
                                key_values_buffer.extend(incoming_key_values.unwrap_or_default().into_iter().map(|(k, v)| (k, Poll::Ready(Some(v)))));

                                if eof {
                                    // If this is the last activation,
                                    // signal that all keys have
                                    // terminated.
                                    keys_buffer.extend(key_values_buffer.iter().map(|(k, _v)| k).cloned());
                                    keys_buffer.extend(state_cache.keys().cloned());
                                    keys_buffer.dedup();
                                    // Drain to re-use allocation.
                                    key_values_buffer.extend(keys_buffer.drain(..).map(|k| (k.clone(), Poll::Ready(None))));
                                } else {
                                    // Otherwise, wake up any keys
                                    // that are passed their requested
                                    // activation time.
                                    key_values_buffer.extend(key_to_next_activate_at_buffer.iter().filter(|(_k, a)| a.elapsed() >= Duration::ZERO).map(|(k, _a)| (k.clone(), Poll::Pending)));
                                }

                                // Drain to re-use allocation.
                                for (key, next_value) in key_values_buffer.drain(..) {
                                    let state = state_cache.remove(&key);
                                    // Remove any activation times
                                    // this current one will satisfy.
                                    if let Entry::Occupied(next_activate_at_entry) = key_to_next_activate_at_buffer.entry(key.clone()) {
                                        if next_activate_at_entry.get().elapsed() >= Duration::ZERO {
                                            next_activate_at_entry.remove();
                                        }
                                    }

                                    let (updated_state, output, activate_after) = logic(&key, state, next_value);

                                    let state_update = match updated_state {
                                        Some(state) => StateUpdate::Upsert(state),
                                        None => StateUpdate::Reset,
                                    };
                                    match &state_update {
                                        StateUpdate::Upsert(state) => {
                                            state_cache.insert(key.clone(), state.clone())
                                        }
                                        StateUpdate::Reset => state_cache.remove(&key),
                                    };
                                    outgoing_epoch_to_key_updates_buffer
                                        .entry(epoch.clone())
                                        .or_insert_with(HashMap::new)
                                        .insert(key.clone(), state_update);

                                    output_session.give_iterator(
                                        output.into_iter().map(|item| (key.clone(), item)),
                                    );

                                    if let Some(activate_after) = activate_after {
                                        let activate_at = Instant::now() + activate_after;
                                        key_to_next_activate_at_buffer.entry(key.clone())
                                            .and_modify(|next_activate_at: &mut Instant| if activate_at < *next_activate_at { *next_activate_at = activate_at; })
                                            .or_insert(activate_at);
                                    }
                                }
                            }
                        }

                        // Output a final state update output per
                        // epoch. Remove will ensure we slowly drain
                        // the buffer.
                        if is_closed(&epoch) {
                            if let Some(key_updates) =
                                outgoing_epoch_to_key_updates_buffer.remove(&epoch)
                            {
                                let mut state_backup_session =
                                    state_backup_handle.session(&state_backup_cap);

                                let backups = key_updates.into_iter().map(|(key, update)| {
                                    StateBackup(
                                        RecoveryKey(step_id.clone(), key, epoch.clone()),
                                        update,
                                    )
                                });

                                state_backup_session.give_iterator(backups);
                            }
                        }
                    }

                    // Schedule an activation at the next requested
                    // wake up time.
                    let now = Instant::now();
                    if let Some(delay) = key_to_next_activate_at_buffer.values().map(|a| a.duration_since(now)).min() {
                        activator.activate_after(delay);
                    }
                }

                if eof {
                    output_cap = None;
                    state_backup_cap = None;
                }
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
    builder: &TdPyCallable,
    mapper: &TdPyCallable,
    key: &StateKey,
    state: Option<TdPyAny>,
    next_value: Poll<Option<TdPyAny>>,
) -> (
    Option<TdPyAny>,
    impl IntoIterator<Item = TdPyAny>,
    Option<Duration>,
) {
    if let Poll::Ready(Some(value)) = next_value {
        let state = state.unwrap_or_else(|| build(builder, key));

        Python::with_gil(|py| {
            let (updated_state, updated_value): (Option<TdPyAny>, TdPyAny) = with_traceback!(py, {
                let updated_state_value_pytuple: TdPyAny = mapper
                    .call1(py, (state.clone_ref(py), value.clone_ref(py)))?
                    .into();
                updated_state_value_pytuple.extract(py)
                        .map_err(|_err| PyTypeError::new_err(format!("return value of `mapper` in stateful map operator must be a 2-tuple of `(updated_state, updated_value)`; got `{updated_state_value_pytuple:?}` instead")))
            });
            debug!("stateful_map for key={key:?}: mapper={mapper:?}(state={state:?}, value={value:?}) -> (updated_state={updated_state:?}, updated_value={updated_value:?})");

            (updated_state, Some(updated_value), None)
        })
    } else {
        (state, None, None)
    }
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
pub(crate) fn state_source<S: Scope, D: Data + Debug>(
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
